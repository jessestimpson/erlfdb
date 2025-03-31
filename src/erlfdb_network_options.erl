-module(erlfdb_network_options).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc hidden.
-endif.

-export([get_defaults/0, merge/2, compile_time_external_client_directory/0]).

-ifdef(erlfdb_compile_time_external_client_directory).
% The content of this defined var lives in rebar.config.script
-define(EXTERNAL_CLIENT_DIRECTORY_DEFAULT,
    list_to_binary(?erlfdb_compile_time_external_client_directory)
).
-else.
-define(EXTERNAL_CLIENT_DIRECTORY_DEFAULT, <<>>).
-endif.

get_defaults() ->
    [
        {ignore_external_client_failures, true},
        {callbacks_on_external_threads, true},
        {external_client_directory,
            {erlfdb_network_options, compile_time_external_client_directory, []}},
        {client_threads_per_version, 1}
    ].

merge(A, B) ->
    L = lists:foldl(
        fun(O, BAcc) ->
            Key =
                case O of
                    {K, _} -> K;
                    K -> K
                end,
            case proplists:is_defined(Key, BAcc) of
                true ->
                    BAcc;
                false ->
                    [O | BAcc]
            end
        end,
        B,
        A
    ),
    lists:reverse(L).

compile_time_external_client_directory() ->
    ?EXTERNAL_CLIENT_DIRECTORY_DEFAULT.
