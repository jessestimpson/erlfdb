-module(erlfdb_sandbox).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc """
Creates a database that is to be used as a sandbox.

The sandbox database is contructed using a single
fdbserver process and some default settings are selected.
""".
-endif.

-export([open/0]).

-define(Options, [
    {dir, <<".erlfdb_sandbox">>},
    {cluster_name, <<"erlfdbsandbox">>},
    {cluster_id, <<"erlfdbsandbox">>}
]).

-if(?DOCATTRS).
-doc """
Opens the sandbox database.
""".
-endif.
open() ->
    erlfdb_util:get_test_db(?Options).
