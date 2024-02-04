% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_util).

-export([
    get_test_db/0,
    get_test_db/1,

    init_test_cluster/1,

    create_and_open_test_tenant/2,
    clear_and_delete_test_tenant/1,

    get/2,
    get/3,

    repr/1,

    debug_cluster/1,
    debug_cluster/3
]).

% Some systems are unable to compute the shared machine id without root,
% so we'll provide a hardcoded machine id for our managed fdbserver
-define(TEST_CLUSTER_MACHINE_ID, <<?MODULE_STRING>>).
-define(TEST_TENANT_NAME, <<?MODULE_STRING, ".test">>).

get_test_db() ->
    get_test_db([]).

get_test_db(Options) ->
    {ok, ClusterFile} = init_test_cluster(Options),
    erlfdb:open(ClusterFile).

init_test_cluster(Options) ->
    % Hack to ensure erlfdb app environment is loaded during unit tests
    ok = application:ensure_started(erlfdb),
    case application:get_env(erlfdb, test_cluster_file) of
        {ok, system_default} ->
            {ok, <<>>};
        {ok, ClusterFile} ->
            {ok, ClusterFile};
        undefined ->
            init_test_cluster_int(Options)
    end.

create_and_open_test_tenant(Db, Options) ->
    case proplists:get_value(empty, Options) of
        true ->
            clear_test_tenant(Db);
        _ ->
            ok
    end,
    erlfdb_tenant_management:transactional(Db,
        fun(Tx) ->
                case erlfdb:wait(erlfdb_tenant_management:get_tenant(Tx, ?TEST_TENANT_NAME)) of
                    not_found ->
                        erlfdb_tenant_management:create_tenant(Tx, ?TEST_TENANT_NAME);
                    _ ->
                        ok
                end
        end),
    erlfdb:open_tenant(Db, ?TEST_TENANT_NAME).

clear_test_tenant(Db) ->
    TenantClearFun =
        fun(Tx) ->
            case erlfdb:wait(erlfdb:get_range(Tx, <<>>, <<16#FF>>, [{limit, 1}])) of
                [] ->
                    ok;
                _ ->
                    erlfdb:clear_range(Tx, <<>>, <<16#FE, 16#FF, 16#FF, 16#FF>>)
            end
        end,
    Tenant = erlfdb:open_tenant(Db, ?TEST_TENANT_NAME),
    erlfdb:transactional(Tenant, TenantClearFun).

clear_and_delete_test_tenant(Db) ->
    TenantDeleteFun =
        fun(Tx) ->
            case erlfdb:wait(erlfdb_tenant_management:get_tenant(Tx, ?TEST_TENANT_NAME)) of
                not_found ->
                    ok;
                _ ->
                    % embedded transaction
                    clear_test_tenant(Db),

                    erlfdb_tenant_management:delete_tenant(Db, ?TEST_TENANT_NAME)
            end
        end,
    erlfdb_tenant_management:transactional(Db, TenantDeleteFun).

get(List, Key) ->
    get(List, Key, undefined).

get(List, Key, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} -> Value;
        _ -> Default
    end.

repr(Bin) when is_binary(Bin) ->
    [$'] ++
        lists:map(
            fun(C) ->
                case C of
                    9 -> "\\t";
                    10 -> "\\n";
                    13 -> "\\r";
                    39 -> "\\'";
                    92 -> "\\\\";
                    _ when C >= 32, C =< 126 -> C;
                    _ -> io_lib:format("\\x~2.16.0b", [C])
                end
            end,
            binary_to_list(Bin)
        ) ++ [$'].

debug_cluster(Tx) ->
    debug_cluster(Tx, <<>>, <<16#FE, 16#FF, 16#FF>>).

debug_cluster(Tx, Start, End) ->
    lists:foreach(
        fun({Key, Val}) ->
            io:format(standard_error, "~s => ~s~n", [
                string:pad(erlfdb_util:repr(Key), 60),
                repr(Val)
            ])
        end,
        erlfdb:get_range(Tx, Start, End)
    ).

init_test_cluster_int(Options) ->
    {ok, CWD} = file:get_cwd(),
    DefaultIpAddr = {127, 0, 0, 1},
    DefaultPort = get_available_port(),
    DefaultDir = filename:join(CWD, ".erlfdb"),

    IpAddr = ?MODULE:get(Options, ip_addr, DefaultIpAddr),
    Port = ?MODULE:get(Options, port, DefaultPort),
    Dir = ?MODULE:get(Options, dir, DefaultDir),
    ClusterName = ?MODULE:get(Options, cluster_name, <<"erlfdbtest">>),
    ClusterId = ?MODULE:get(Options, cluster_id, <<"erlfdbtest">>),

    DefaultClusterFile = filename:join(Dir, <<"erlfdb.cluster">>),
    ClusterFile = ?MODULE:get(Options, cluster_file, DefaultClusterFile),

    write_cluster_file(ClusterFile, ClusterName, ClusterId, IpAddr, Port),

    FDBServerBin = find_fdbserver_bin(Options),

    {FDBPid, _} = spawn_monitor(fun() ->
        % Open the fdbserver port
        FDBPortName = {spawn_executable, FDBServerBin},
        FDBPortArgs = [
            <<"-p">>,
            ip_port_to_str(IpAddr, Port),
            <<"-C">>,
            ClusterFile,
            <<"-d">>,
            Dir,
            <<"-L">>,
            Dir,
            <<"-i">>,
            ?TEST_CLUSTER_MACHINE_ID
        ],
        FDBPortOpts = [{args, FDBPortArgs}],
        FDBServer = erlang:open_port(FDBPortName, FDBPortOpts),
        {os_pid, FDBPid} = erlang:port_info(FDBServer, os_pid),

        % Open the monitor pid
        MonitorPath = get_monitor_path(),
        ErlPid = os:getpid(),

        MonitorPortName = {spawn_executable, MonitorPath},
        MonitorPortArgs = [{args, [ErlPid, integer_to_binary(FDBPid)]}],
        Monitor = erlang:open_port(MonitorPortName, MonitorPortArgs),

        init_fdb_db(ClusterFile, Options),

        receive
            {wait_for_init, ParentPid} ->
                ParentPid ! {initialized, self()}
        after 5000 ->
            true = erlang:port_close(FDBServer),
            true = erlang:port_close(Monitor),
            erlang:error(fdb_parent_died)
        end,

        port_loop(FDBServer, Monitor),

        true = erlang:port_close(FDBServer),
        true = erlang:port_close(Monitor)
    end),

    FDBPid ! {wait_for_init, self()},
    receive
        {initialized, FDBPid} ->
            ok;
        Msg ->
            erlang:error({fdbserver_error, Msg})
    end,

    ok = application:set_env(erlfdb, test_cluster_file, ClusterFile),
    ok = application:set_env(erlfdb, test_cluster_pid, FDBPid),
    {ok, ClusterFile}.

get_available_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    Port.

find_fdbserver_bin(Options) ->
    Locations =
        case ?MODULE:get(Options, fdbserver_bin) of
            undefined ->
                [
                    <<"/usr/sbin/fdbserver">>,
                    <<"/usr/local/bin/fdbserver">>,
                    <<"/usr/local/sbin/fdbserver">>,
                    <<"/usr/local/libexec/fdbserver">>
                ];
            Else ->
                [Else]
        end,
    case lists:filter(fun filelib:is_file/1, Locations) of
        [Path | _] -> Path;
        [] -> erlang:error(fdbserver_bin_not_found)
    end.

write_cluster_file(FileName, ClusterName, ClusterId, IpAddr, Port) ->
    Args = [ClusterName, ClusterId, ip_port_to_str(IpAddr, Port)],
    Contents = io_lib:format("~s:~s@~s~n", Args),
    ok = filelib:ensure_dir(FileName),
    ok = file:write_file(FileName, iolist_to_binary(Contents)).

get_monitor_path() ->
    PrivDir =
        case code:priv_dir(erlfdb) of
            {error, _} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
            Path ->
                Path
        end,
    filename:join(PrivDir, "monitor.py").

init_fdb_db(ClusterFile, Options) ->
    DefaultFDBCli = os:find_executable("fdbcli"),
    FDBCli =
        case ?MODULE:get(Options, fdbcli_bin, DefaultFDBCli) of
            false -> erlang:error(fdbcli_not_found);
            DefaultFDBCli -> "fdbcli";
            FDBCli0 -> FDBCli0
        end,
    Fmt = "~s -C ~s --exec \"configure new single ssd tenant_mode=optional_experimental\"",
    Cmd = lists:flatten(io_lib:format(Fmt, [FDBCli, ClusterFile])),
    case os:cmd(Cmd) of
        "Database created" ++ _ -> ok;
        "ERROR: Database already exists!" ++ _ -> ok;
        Msg -> erlang:error({fdb_init_error, Msg})
    end.

port_loop(FDBServer, Monitor) ->
    receive
        close ->
            ok;
        {FDBServer, {data, "FDBD joined cluster.\n"}} ->
            % Silence start message
            port_loop(FDBServer, Monitor);
        {Port, {data, Msg}} when Port == FDBServer orelse Port == Monitor ->
            io:format(standard_error, "~p", [Msg]),
            port_loop(FDBServer, Monitor);
        Error ->
            erlang:exit({fdb_cluster_error, Error})
    end.

ip_port_to_str({I1, I2, I3, I4}, Port) ->
    Fmt = "~b.~b.~b.~b:~b",
    iolist_to_binary(io_lib:format(Fmt, [I1, I2, I3, I4, Port])).
