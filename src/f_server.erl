-module(f_server).
-behaviour(gen_server).

-export([
    start/3, start/4,
    start_link/3, start_link/4,
    cast/2,
    cast_k/2,
    priority_cast/2,
    priority_call/2, priority_call/3,
    call/2, call/3,
    kill/2
]).

-callback init(Args :: term()) -> {ok, State :: term()} | {error, Reason :: term()}.
-callback key(State :: term()) -> Key :: tuple().
-callback handle_cast(Msg :: term(), State :: term()) -> {noreply, NewState :: term()}.
-callback handle_call(Request :: term(), From :: term(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} | {noreply, NewState :: term()}.
-callback handle_info(Info :: term(), State :: term()) -> {noreply, NewState :: term()}.

-optional_callbacks([key/1, handle_cast/2, handle_call/3, handle_info/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DefaultKey(Mod), {<<"f_server">>, atom_to_binary(Mod)}).
-define(TxCallbackTimeout, 5000).
-define(VS(Tx), {
    versionstamp,
    16#ffffffffffffffff,
    16#ffff,
    erlfdb:get_next_tx_id(Tx)
}).

-record(state, {db, mod, key, watch}).

start(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(?MODULE, {get_db(Opts), Mod, Arg, Consume, Reset}, Opts).

start(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start(Reg, ?MODULE, {get_db(Opts), Mod, Arg, Consume, Reset}, Opts).

start_link(Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(?MODULE, {get_db(Opts), Mod, Arg, Consume, Reset}, Opts).

start_link(Reg, Mod, Arg, Opts) ->
    Consume = proplists:get_value(consume, Opts, true),
    Reset = proplists:get_value(reset, Opts, false),
    gen_server:start_link(Reg, ?MODULE, {get_db(Opts), Mod, Arg, Consume, Reset}, Opts).

cast(Server, Request) ->
    cast_k(Server, [Request]).

cast_k(Server, Requests) ->
    gen_server:cast(Server, {cast, Requests}).

call(Server, Request) ->
    call(Server, Request, 5000).

call(Server, Request, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    CallResult =
        try
            gen_server:call(Server, {call, Request, self()}, Timeout)
        catch
            error:{timeout, _} ->
                % @todo: If CallKey was created, it will be abandoned
                {noreply, {undefined, undefined, undefined}}
        end,

    case CallResult of
        {reply, Reply} ->
            Reply;
        {noreply, {Db, CallKey, Watch}} ->
            T2 = erlang:monotonic_time(millisecond),

            case await_call_reply(Db, CallKey, Watch, Timeout - (T2 - T1)) of
                {error, timeout} ->
                    erlang:error(timeout);
                Reply ->
                    Reply
            end
    end.

priority_cast(Server, Request) ->
    gen_server:cast(Server, {priority, Request}).

priority_call(Server, Request) ->
    gen_server:call(Server, {priority, Request}).

priority_call(Server, Request, Timeout) ->
    gen_server:call(Server, {priority, Request}, Timeout).

kill(Server, Reason) ->
    gen_server:cast(Server, {kill, Reason}).

get_db(Opts) ->
    case proplists:get_value(db, Opts) of
        undefined ->
            erlfdb:open();
        Db ->
            Db
    end.

init({Db, Mod, Arg, Consume, Reset}) ->
    case Mod:init(Arg) of
        {ok, InitialState} ->
            State0 = #state{db = Db, mod = Mod},
            State = State0#state{
                key = invoke_pure_callback(key, [InitialState], State0, ?DefaultKey(Mod))
            },
            erlfdb:transactional(Db, fun(Tx) ->
                case {Reset, tx_get_mod_state(Tx, State)} of
                    {false, {ok, _ModState}} ->
                        ok;
                    {true, _ModState} ->
                        tx_set_mod_state(Tx, InitialState, State);
                    {_, {error, not_found}} ->
                        tx_set_mod_state(Tx, InitialState, State)
                end
            end),
            [gen_server:cast(self(), consume) || Consume],
            {ok, State};
        Other ->
            Other
    end.

handle_call(
    {call, Request, WatchTo}, _LocalFrom, State = #state{db = Db, key = Key, watch = undefined}
) ->
    % if there's no watch, then assume the queue is nonempty, and push the request
    WaitingKey = get_waiting_key(Key),
    From = get_from(WaitingKey, make_ref()),
    {CallKey, Watch} = erlfdb:transactional(Db, fun(Tx) ->
        tx_push_call(Tx, Key, Request, From, WatchTo)
    end),
    {reply, {Db, CallKey, Watch}, State};
handle_call(
    {call, Request, WatchTo}, _LocalFrom, State = #state{db = Db, key = Key, watch = _ConsumeWatch}
) ->
    % if there's a watch, we pay for the queue length check, assuming it will be 0 in most cases. If
    % it is zero, then we can handle the call immediately without pushing it onto the queue.
    WaitingKey = get_waiting_key(Key),
    From = get_from(WaitingKey, make_ref()),
    {Resp, Actions} = erlfdb:transactional(Db, fun(Tx) ->
        case tx_queue_len(Tx, Key) of
            0 ->
                case tx_invoke_tx_callback(Tx, handle_call, [Request, From], State) of
                    {error, Reason = {function_not_exported, _}} ->
                        erlang:error(Reason);
                    {ok, {{reply, Reply, State2}, Actions}} ->
                        {{reply, {reply, Reply}, State2}, Actions}
                end;
            _Len ->
                {CallKey, Watch} = tx_push_call(Tx, Key, Request, From, WatchTo),
                {{reply, {noreply, {Db, CallKey, Watch}}, State}, []}
        end
    end),
    _ = handle_actions(Actions),
    Resp;
handle_call({priority, Request}, _From, State = #state{db = Db}) ->
    From = make_ref(),
    {Actions, Reply, State2} = erlfdb:transactional(Db, fun(Tx) ->
        case tx_invoke_tx_callback(Tx, handle_call, [Request, From], State) of
            {error, Reason = {function_not_exported, _}} ->
                erlang:error(Reason);
            {ok, {{reply, Reply, State2}, Actions}} ->
                {Actions, Reply, State2}
        end
    end),
    _ = handle_actions(Actions),
    {reply, Reply, State2}.

handle_cast(consume, State = #state{db = Db, key = Key}) ->
    % 1 at a time because we are limited to 5 seconds per transaction
    K = 1,
    {Watch, Actions, State2} =
        erlfdb:transactional(
            Db,
            fun(Tx) ->
                case tx_consume_k(Tx, K, Key) of
                    {[{call, Request, From}], Watch} ->
                        case tx_invoke_tx_callback(Tx, handle_call, [Request, From], State) of
                            {error, Reason = {function_not_exported, _}} ->
                                erlang:error(Reason);
                            {ok, {{reply, Reply, State2}, Actions}} ->
                                erlfdb:set(
                                    Tx, erlfdb_tuple:pack(From), term_to_binary({reply, Reply})
                                ),
                                {Watch, Actions, State2}
                        end;
                    {[{cast, Request}], Watch} ->
                        case tx_invoke_tx_callback(Tx, handle_cast, [Request], State) of
                            {error, Reason = {function_not_exported, _}} ->
                                erlang:error(Reason);
                            {ok, {{noreply, State2}, Actions}} ->
                                {Watch, Actions, State2}
                        end;
                    {[], Watch} ->
                        {Watch, [], State}
                end
            end
        ),
    _ = handle_actions(Actions),
    case Watch of
        undefined ->
            gen_server:cast(self(), consume);
        _ ->
            ok
    end,
    {noreply, State2#state{watch = Watch}};
handle_cast({cast, Requests}, State = #state{db = Db, key = Key}) ->
    erlfdb:transactional(Db, fun(Tx) ->
        tx_push_k(Tx, Key, [{cast, Request} || Request <- Requests])
    end),
    {noreply, State};
handle_cast({priority, Request}, State = #state{db = Db}) ->
    {Actions, State2} = erlfdb:transactional(Db, fun(Tx) ->
        case tx_invoke_tx_callback(Tx, handle_cast, [Request], State) of
            {error, Reason = {function_not_exported, _}} ->
                erlang:error(Reason);
            {ok, {{noreply, State2}, Actions}} ->
                {Actions, State2}
        end
    end),
    _ = handle_actions(Actions),
    {noreply, State2};
handle_cast({kill, Reason}, State = #state{db = Db, key = Key}) ->
    erlfdb:transactional(Db, fun(Tx) -> tx_delete(Tx, Key) end),
    {stop, Reason, State}.

handle_info({Ref, ready}, State = #state{watch = {erlfdb_future, Ref, _}}) ->
    handle_cast(consume, State#state{watch = undefined});
handle_info(Info, State = #state{db = Db}) ->
    {Actions, State2} = erlfdb:transactional(Db, fun(Tx) ->
        case tx_invoke_tx_callback(Tx, handle_info, [Info], State) of
            {error, {function_not_exported, _}} ->
                {[], State};
            {ok, {{noreply, State2}, Actions}} ->
                {Actions, State2}
        end
    end),
    _ = handle_actions(Actions),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

tx_invoke_tx_callback(Tx, Callback, Args, State = #state{mod = Mod}) ->
    T1 = erlang:monotonic_time(millisecond),
    Arity = length(Args) + 1,
    Result =
        case erlang:function_exported(Mod, Callback, Arity) of
            true ->
                case tx_get_mod_state(Tx, State) of
                    {ok, ModState} ->
                        Args2 = modify_args_for_callback(Callback, Args, ModState),
                        CallbackResult = erlang:apply(Mod, Callback, Args2),
                        {ok,
                            tx_handle_callback_result(
                                Tx, Callback, CallbackResult, ModState, State
                            )};
                    {error, not_found} ->
                        {error, {mod_state_not_found, Mod}}
                end;
            false ->
                {error, {function_not_exported, {Mod, Callback, Arity}}}
        end,
    T2 = erlang:monotonic_time(millisecond),
    if
        T2 - T1 > ?TxCallbackTimeout ->
            erlang:error(tooslow);
        true ->
            Result
    end.

invoke_pure_callback(Callback, Args, State, Default) ->
    case invoke_pure_callback(Callback, Args, State) of
        {ok, Result} ->
            Result;
        {error, {function_not_exported, _}} ->
            Default
    end.

invoke_pure_callback(Callback, Args, #state{mod = Mod}) ->
    Arity = length(Args),
    case erlang:function_exported(Mod, Callback, Arity) of
        true ->
            {ok, erlang:apply(Mod, Callback, Args)};
        false ->
            {error, {function_not_exported, {Mod, Callback, Arity}}}
    end.

modify_args_for_callback(_, Args, ModState) ->
    Args ++ [ModState].

tx_delete(Tx, Key) ->
    tx_clear_mod_state(Tx, Key),
    tx_delete_queue(Tx, Key),
    WaitingKey = get_waiting_key(Key),
    {SK, EK} = erlfdb_tuple:range(WaitingKey),
    erlfdb:clear_range(Tx, SK, EK).

tx_clear_mod_state(Tx, Key) ->
    StateKey = get_state_key(Key),
    {SK, EK} = erlfdb_tuple:range(StateKey),
    erlfdb:clear_range(Tx, SK, EK).

tx_get_mod_state(Tx, _State = #state{key = Key}) ->
    StateKey = get_state_key(Key),
    {SK, EK} = erlfdb_tuple:range(StateKey),
    case erlfdb:get_range(Tx, SK, EK, [{wait, true}]) of
        [] ->
            {error, not_found};
        KVs ->
            {_, Vs} = lists:unzip(KVs),
            {ok, binary_to_term(iolist_to_binary(Vs))}
    end.

tx_set_mod_state(_Tx, ModState, ModState, _State) ->
    ok;
tx_set_mod_state(Tx, _OrigModState, ModState, State) ->
    tx_set_mod_state(Tx, ModState, State).

tx_set_mod_state(Tx, ModState, _State = #state{key = Key}) ->
    Bin = term_to_binary(ModState),
    Chunks = binary_chunk_every(Bin, 100000, []),
    StateKey = get_state_key(Key),
    {ChunkKeys, {FirstUnused, EK}} = partition_chunked_key(StateKey, length(Chunks)),
    [erlfdb:set(Tx, erlfdb_tuple:pack(K), Chunk) || {K, Chunk} <- lists:zip(ChunkKeys, Chunks)],
    erlfdb:clear_range(Tx, erlfdb_tuple:pack(FirstUnused), EK),
    ok.

tx_handle_callback_result(Tx, handle_cast, {noreply, ModState}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{noreply, State}, []};
tx_handle_callback_result(Tx, handle_cast, {noreply, ModState, Actions}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{noreply, State}, Actions};
tx_handle_callback_result(Tx, handle_call, {reply, Reply, ModState}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{reply, Reply, State}, []};
tx_handle_callback_result(Tx, handle_call, {reply, Reply, ModState, Actions}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{reply, Reply, State}, Actions};
tx_handle_callback_result(Tx, handle_info, {noreply, ModState}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{noreply, State}, []};
tx_handle_callback_result(Tx, handle_info, {noreply, ModState, Actions}, OrigModState, State) ->
    tx_set_mod_state(Tx, OrigModState, ModState, State),
    {{noreply, State}, Actions}.

tx_push_call(Tx, Key, Request, From, WatchTo) ->
    CallKey = erlfdb_tuple:pack(From),
    erlfdb:set(Tx, CallKey, term_to_binary(noreply)),
    Future = erlfdb:watch(Tx, CallKey, [{to, WatchTo}]),
    tx_push_k(Tx, Key, [{call, Request, From}]),
    {CallKey, Future}.

tx_delete_queue(Tx, Key) ->
    QueueKey = get_queue_key(Key),
    {SK, EK} = erlfdb_tuple:range(QueueKey),
    erlfdb:clear_range(Tx, SK, EK).

tx_push_k(Tx, Key, Items) ->
    QueueKey = get_queue_key(Key),
    ItemKey = get_item_key(QueueKey),
    ItemKey2 = erlang:insert_element(1 + tuple_size(ItemKey), ItemKey, undefined),
    [
        erlfdb:set_versionstamped_key(
            Tx,
            erlfdb_tuple:pack_vs(
                erlang:setelement(
                    tuple_size(ItemKey2),
                    ItemKey2,
                    ?VS(Tx)
                )
            ),
            term_to_binary(Item)
        )
     || Item <- Items
    ],

    PushKey = get_push_key(QueueKey),
    erlfdb:add(Tx, erlfdb_tuple:pack(PushKey), length(Items)).

tx_pop_k(Tx, K, Key) ->
    QueueKey = get_queue_key(Key),
    ItemKey = get_item_key(QueueKey),
    {QS, QE} = erlfdb_tuple:range(ItemKey),
    case erlfdb:get_range(Tx, QS, QE, [{limit, K}, {wait, true}]) of
        [] ->
            {{error, empty}, []};
        KVs = [{S, _} | _] ->
            N = length(KVs),
            {E, _} = lists:last(KVs),
            erlfdb:clear_range(Tx, S, erlfdb_key:strinc(E)),
            PopKey = get_pop_key(QueueKey),
            erlfdb:add(Tx, erlfdb_tuple:pack(PopKey), N),

            Status =
                if
                    N == K -> ok;
                    true -> {error, empty}
                end,
            {Status, [binary_to_term(V) || {_, V} <- KVs]}
    end.

tx_queue_len(Tx, Key) ->
    QueueKey = get_queue_key(Key),
    PushKey = get_push_key(QueueKey),
    PopKey = get_pop_key(QueueKey),
    F = [erlfdb:get(Tx, erlfdb_tuple:pack(PushKey)), erlfdb:get(Tx, erlfdb_tuple:pack(PopKey))],
    [Push, Pop] = erlfdb:wait_for_all(F),
    decode_as_int(Push, 0) - decode_as_int(Pop, 0).

decode_as_int(not_found, Default) -> Default;
decode_as_int(Val, _Default) -> binary:decode_unsigned(Val, little).

tx_consume_k(Tx, K, Key) ->
    case tx_pop_k(Tx, K, Key) of
        {ok, Vals} ->
            {Vals, undefined};
        {{error, empty}, Vals} ->
            {Vals, tx_watch(Tx, Key)}
    end.

tx_watch(Tx, Key) ->
    QueueKey = get_queue_key(Key),
    PushKey = get_push_key(QueueKey),
    erlfdb:watch(Tx, erlfdb_tuple:pack(PushKey)).

binary_chunk_every(<<>>, _Size, Acc) ->
    lists:reverse(Acc);
binary_chunk_every(Bin, Size, Acc) ->
    case Bin of
        <<Chunk:Size/binary, Rest/binary>> ->
            binary_chunk_every(Rest, Size, [Chunk | Acc]);
        Chunk ->
            lists:reverse([Chunk | Acc])
    end.

handle_actions([]) ->
    ok;
handle_actions([Action | Actions]) ->
    Action(),
    handle_actions(Actions).

get_state_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"s">>).

get_queue_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"q">>).

get_item_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"i">>).

get_push_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"n">>).

get_pop_key(QueueKey) ->
    erlang:insert_element(1 + tuple_size(QueueKey), QueueKey, <<"p">>).

get_waiting_key(Tuple) ->
    erlang:insert_element(1 + tuple_size(Tuple), Tuple, <<"c">>).

get_from(WaitingKey, Ref) ->
    Bin = term_to_binary(Ref),
    erlang:insert_element(1 + tuple_size(WaitingKey), WaitingKey, Bin).

partition_chunked_key(BaseKey, N) ->
    {_, EK} = erlfdb_tuple:range(BaseKey),
    ChunkKey = erlang:insert_element(1 + tuple_size(BaseKey), BaseKey, 0),
    ChunkKeys = [erlang:setelement(tuple_size(ChunkKey), ChunkKey, X) || X <- lists:seq(0, N - 1)],
    FirstUnused = erlang:setelement(tuple_size(ChunkKey), ChunkKey, N),
    {ChunkKeys, {FirstUnused, EK}}.

await_call_reply(undefined, _CallKey, _Watch, _Timeout) ->
    {error, timeout};
await_call_reply(_Db, _CallKey, _Watch, Timeout) when Timeout =< 0 -> {error, timeout};
await_call_reply(Db, CallKey, {erlfdb_future, WatchRef, _}, Timeout) ->
    T1 = erlang:monotonic_time(millisecond),

    Result =
        receive
            {WatchRef, ready} ->
                erlfdb:transactional(Db, fun(Tx) ->
                    Future = erlfdb:get(Db, CallKey),
                    case erlfdb:wait(Future) of
                        not_found ->
                            {watch, erlfdb:watch(Db, CallKey)};
                        Value ->
                            CallState = binary_to_term(Value),
                            case CallState of
                                {reply, Reply} ->
                                    erlfdb:clear(Tx, CallKey),
                                    {reply, Reply};
                                noreply ->
                                    {watch, erlfdb:watch(Db, CallKey)};
                                _ ->
                                    {error, timeout}
                            end
                    end
                end)
        after Timeout ->
            {error, timeout}
        end,

    T2 = erlang:monotonic_time(millisecond),

    case Result of
        {reply, Reply} ->
            Reply;
        {watch, Watch} ->
            await_call_reply(Db, CallKey, Watch, Timeout - (T2 - T1));
        {error, timeout} ->
            {error, timeout}
    end.
