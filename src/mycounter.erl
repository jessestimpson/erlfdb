-module(mycounter).
-behaviour(f_server).

-export([start_link/1, increment/1, decrement/1, get_value/1, dirty_get_value/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Opts) ->
    f_server:start_link(?MODULE, [], Opts).

increment(Server) ->
    f_server:cast(Server, increment).

decrement(Server) ->
    f_server:cast(Server, decrement).

get_value(Server) ->
    f_server:call(Server, get_value).

dirty_get_value(Server) ->
    f_server:priority_call(Server, get_value).

init([]) ->
    {ok, 0}.

handle_call(get_value, _From, State) ->
    {reply, State, State}.

handle_cast(increment, State) ->
    {noreply, State + 1};
handle_cast(decrement, State) ->
    {noreply, State - 1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
