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

-module(erlfdb_02_anon_fdbserver_test).

-include_lib("eunit/include/eunit.hrl").

basic_init_test() ->
    {ok, ClusterFile} = erlfdb_util:init_test_cluster([]),
    ?assert(is_binary(ClusterFile)).

basic_open_test() ->
    {ok, ClusterFile} = erlfdb_util:init_test_cluster([]),
    Db = erlfdb:open(ClusterFile),
    erlfdb:transactional(Db, fun(_Tx) ->
        ?assert(true)
    end).

get_db_test() ->
    Db = erlfdb_util:get_test_db(),
    erlfdb:transactional(Db, fun(_Tx) ->
        ?assert(true)
    end).

get_set_get_test() ->
    Db = erlfdb_util:get_test_db(),
    get_set_get(Db).

get_empty_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant1 = erlfdb_util:create_and_open_test_tenant(Db, []),
    Key = gen_key(8),
    Val = crypto:strong_rand_bytes(8),
    erlfdb:transactional(Tenant1, fun(Tx) ->
        ok = erlfdb:set(Tx, Key, Val)
    end),
    erlfdb:transactional(Tenant1, fun(Tx) ->
        ?assertEqual(Val, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),

    % Check we can get an empty db
    Tenant2 = erlfdb_util:create_and_open_test_tenant(Db, [empty]),
    erlfdb:transactional(Tenant2, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),

    % And check state that the old db handle is
    % the same
    erlfdb:transactional(Tenant1, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end).

get_set_get_tenant_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant = erlfdb_util:create_and_open_test_tenant(Db, []),
    get_set_get(Tenant),
    erlfdb_util:clear_and_delete_test_tenant(Db).

get_range_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant = erlfdb_util:create_and_open_test_tenant(Db, []),

    KVs = [
        {{<<"get_range_test">>, 1}, {<<"get_range_test">>, <<"A">>}},
        {{<<"get_range_test">>, 2}, {<<"get_range_test">>, <<"B">>}},
        {{<<"get_range_test">>, 3}, {<<"get_range_test">>, <<"C">>}}
    ],
    erlfdb:transactional(Tenant, fun(Tx) ->
        [erlfdb:set(Tx, erlfdb_tuple:pack(K), erlfdb_tuple:pack(V)) || {K, V} <- KVs]
    end),

    {StartKey, EndKey} = erlfdb_tuple:range({<<"get_range_test">>}),
    GetRangeResult = erlfdb:transactional(Tenant, fun(Tx) ->
        PackedKVs = erlfdb:wait(erlfdb:get_range(Tx, StartKey, EndKey)),
        [{erlfdb_tuple:unpack(K), erlfdb_tuple:unpack(V)} || {K, V} <- PackedKVs]
    end),

    ?assertEqual(KVs, GetRangeResult),

    MsgKey = erlfdb_tuple:pack({<<"get_range_test">>, <<"A">>, <<"msg">>}),
    erlfdb:transactional(Tenant, fun(Tx) ->
        erlfdb:set(Tx, MsgKey, <<"hello world">>)
    end),

    Result = erlfdb:transactional(Tenant, fun(Tx) ->
        MStartKey = erlfdb_tuple:pack({<<"get_range_test">>, 1}),
        MEndKey = erlfdb_key:strinc(MStartKey),

        Mapper = {<<"get_range_test">>, <<"{V[1]}">>, <<"{...}">>},
        [{{_PKey, _PValue}, {_SKeyBegin, _SKeyEnd}, [{_Key, Message}]}] = erlfdb:wait(
            erlfdb:get_mapped_range(Tx, MStartKey, MEndKey, Mapper)
        ),
        Message
    end),

    ?assertEqual(<<"hello world">>, Result).

% get_mapped_range requires the use of tuples in the keys/values so that the
% element selector syntax can be used. This test demonstrates the minimal set
% of keys necessary to exercise the feature.
get_mapped_range_minimal_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant = erlfdb_util:create_and_open_test_tenant(Db, []),
    erlfdb:transactional(Tenant, fun(Tx) ->
        erlfdb:set(Tx, <<"a">>, erlfdb_tuple:pack({<<"b">>})),
        erlfdb:set(Tx, erlfdb_tuple:pack({<<"b">>}), <<"c">>)
    end),
    Result = erlfdb:transactional(Tenant, fun(Tx) ->
        erlfdb:get_mapped_range(Tx, <<"a">>, <<"b">>, {<<"{V[0]}">>, <<"{...}">>})
    end),
    ?assertEqual(
        [{{<<"a">>, <<1, $b, 0>>}, {<<1, $b, 0>>, <<1, $b, 1>>}, [{<<1, $b, 0>>, <<"c">>}]}], Result
    ).

get_set_get(DbOrTenant) ->
    Key = gen_key(8),
    Val = crypto:strong_rand_bytes(8),
    erlfdb:transactional(DbOrTenant, fun(Tx) ->
        ?assertEqual(not_found, erlfdb:wait(erlfdb:get(Tx, Key)))
    end),
    erlfdb:transactional(DbOrTenant, fun(Tx) ->
        ?assertEqual(ok, erlfdb:set(Tx, Key, Val))
    end),
    erlfdb:transactional(DbOrTenant, fun(Tx) ->
        ?assertEqual(Val, erlfdb:wait(erlfdb:get(Tx, Key)))
    end).

gen_key(Size) when is_integer(Size), Size > 1 ->
    RandBin = crypto:strong_rand_bytes(Size - 1),
    <<0, RandBin/binary>>.
