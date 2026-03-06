#!/usr/bin/env escript
%%! -pa _build/default/lib/erlfdb/ebin

-mode(compile).

main([ClusterFile, Phase, MvDir, ApiVersionStr]) ->
    ApiVersion = list_to_integer(ApiVersionStr),
    io:format("[~s] Starting verification (api_version=~b)~n", [Phase, ApiVersion]),

    %% Load erlfdb app metadata (does NOT load any modules or the NIF)
    ok = application:load(erlfdb),

    %% Configure multi-version client BEFORE any erlfdb module is loaded.
    %% api_version must be compatible with both old and new FDB versions.
    ok = application:set_env(erlfdb, api_version, ApiVersion),
    ok = application:set_env(erlfdb, network_options, [
        {external_client_directory, list_to_binary(MvDir)},
        {external_client_library, false}
    ]),

    %% Opening the database triggers NIF loading -> init/0 reads our app env,
    %% configures the multi-version client, and starts the FDB network.
    Db = erlfdb:open(list_to_binary(ClusterFile)),

    %% Write a phase-specific key
    Key = iolist_to_binary(["mvtest_", Phase]),
    Val = iolist_to_binary(["value_", Phase]),
    ok = erlfdb:set(Db, Key, Val),

    %% Read it back
    case erlfdb:get(Db, Key) of
        Val ->
            io:format("[~s] Write+read OK: ~s = ~s~n", [Phase, Key, Val]);
        Other ->
            io:format(standard_error, "[~s] FAIL: expected ~p, got ~p~n", [Phase, Val, Other]),
            halt(1)
    end,

    %% After upgrade, verify the key written before upgrade is still readable
    case Phase of
        "post_upgrade" ->
            case erlfdb:get(Db, <<"mvtest_pre_upgrade">>) of
                <<"value_pre_upgrade">> ->
                    io:format("[~s] Cross-version read OK~n", [Phase]);
                Other2 ->
                    io:format(standard_error,
                        "[~s] Cross-version read FAIL: expected ~p, got ~p~n",
                        [Phase, <<"value_pre_upgrade">>, Other2]),
                    halt(1)
            end;
        _ ->
            ok
    end,

    io:format("[~s] PASSED~n", [Phase]),
    halt(0);

main(_) ->
    io:format(standard_error, "Usage: verify_fdb.escript <cluster_file> <phase> <mv_dir> <api_version>~n", []),
    halt(1).
