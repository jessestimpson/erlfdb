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

-module(erlfdb_key).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
Operations on keys, either by your client Layer, or by the FoundationDB server itself.
""".
-endif.

-export([
    to_selector/1,

    last_less_than/1,
    last_less_or_equal/1,
    first_greater_than/1,
    first_greater_or_equal/1,

    strinc/1,

    list_to_ranges/1
]).

-if(?DOCATTRS).
-doc hidden.
-endif.
to_selector(<<_/binary>> = Key) ->
    {Key, gteq};
to_selector({<<_/binary>>, _} = Sel) ->
    Sel;
to_selector({<<_/binary>>, _, _} = Sel) ->
    Sel;
to_selector(Else) ->
    erlang:error({invalid_key_selector, Else}).

-if(?DOCATTRS).
-doc """
Creates a 'last_less_than' key selector.

[FDB Developer Guide | Key Selectors](https://apple.github.io/foundationdb/developer-guide.html#key-selectors)
""".
-endif.
last_less_than(Key) when is_binary(Key) ->
    {Key, lt}.

-if(?DOCATTRS).
-doc """
Creates a 'last_less_or_equal' key selector.

[FDB Developer Guide | Key Selectors](https://apple.github.io/foundationdb/developer-guide.html#key-selectors)
""".
-endif.
last_less_or_equal(Key) when is_binary(Key) ->
    {Key, lteq}.

-if(?DOCATTRS).
-doc """
Creates a 'first_greater_than' key selector.

[FDB Developer Guide | Key Selectors](https://apple.github.io/foundationdb/developer-guide.html#key-selectors)
""".
-endif.
first_greater_than(Key) when is_binary(Key) ->
    {Key, gt}.

-if(?DOCATTRS).
-doc """
Creates a 'first_greater_or_equal' key selector.

[FDB Developer Guide | Key Selectors](https://apple.github.io/foundationdb/developer-guide.html#key-selectors)
""".
-endif.
first_greater_or_equal(Key) when is_binary(Key) ->
    {Key, gteq}.

-if(?DOCATTRS).
-doc """
Performs a lexocographic increment operation on a binary key.

The resulting key is the first possible key that is lexicographically ordered after all keys beginning with the input.

## Examples

```erlang
1> erlfdb_key:strinc(<<0>>).
<<1>>
```

```erlang
1> erlfdb_key:strinc(<<"hello">>).
<<"hellp">>
```

```erlang
1> erlfdb_key:strinc(<<"hello", 16#FF>>).
<<"hellp">>
```
""".
-endif.
strinc(Key) when is_binary(Key) ->
    Prefix = rstrip_ff(Key),
    PrefixLen = size(Prefix),
    Head = binary:part(Prefix, {0, PrefixLen - 1}),
    Tail = binary:at(Prefix, PrefixLen - 1),
    <<Head/binary, (Tail + 1)>>.

rstrip_ff(<<>>) ->
    erlang:error("Key must contain at least one byte not equal to 0xFF");
rstrip_ff(Key) ->
    KeyLen = size(Key),
    case binary:at(Key, KeyLen - 1) of
        16#FF -> rstrip_ff(binary:part(Key, {0, KeyLen - 1}));
        _ -> Key
    end.

-if(?DOCATTRS).
-doc """
Transforms a list of keys into a list of partitioning ranges using the keys as split points.

## Examples

```erlang
1> Keys = [<<"a">>, <<"b">>, <<"c">>].
2> erlfdb_key:list_to_ranges(Keys).
[
    {<<"a">>, <<"b">>},
    {<<"b">>, <<"c">>}
]
```
""".
-endif.
list_to_ranges(Array) when length(Array) < 2 ->
    erlang:error(badarg);
list_to_ranges(Array) ->
    list_to_ranges(Array, []).

list_to_ranges([_EK], Acc) ->
    lists:reverse(Acc);
list_to_ranges([SK, EK | T], Acc) ->
    list_to_ranges([EK | T], [{SK, EK} | Acc]).
