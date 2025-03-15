-module(erlfdb_sandbox).

-define(DOCATTRS, ?OTP_RELEASE >= 27).
-if(?DOCATTRS).
-moduledoc """
Creates a database that is to be used as a sandbox.

The sandbox database is constructed using a single
fdbserver process and some default settings are selected.
""".
-endif.

-export([open/0, open/1, default_options/0]).

-define(DefaultDir, <<".erlfdb_sandbox">>).

-define(Options(Dir), [
    {dir, iolist_to_binary(Dir)},
    {cluster_name, <<"erlfdbsandbox">>},
    {cluster_id, <<"erlfdbsandbox">>}
]).

-if(?DOCATTRS).
-doc """
Gets the default options for the sandbox database.
""".
-endif.
default_options() ->
    ?Options(?DefaultDir).

-if(?DOCATTRS).
-doc """
Opens the sandbox database using the default directory `.erlfdb_sandbox`.
""".
-endif.
open() ->
    erlfdb_util:get_test_db(default_options()).

-if(?DOCATTRS).
-doc """
Opens the sandbox database in a subdirectory of
`.erlfdb_sandbox`, allowing for multiple concurrent sandboxes.
""".
-endif.
open(Subdir) ->
    Dir = filename:join([?DefaultDir, Subdir]),
    erlfdb_util:get_test_db(?Options(Dir)).
