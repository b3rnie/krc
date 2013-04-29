%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Manual testcases for mapreduce
%%%
%%%
%%% Copyright 2013 Kivra AB
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(mapred_tests).

-export([ test_basic/0
        , test_conflict/0
        ]).

%%%_* Macros ===========================================================
-define(KRC, krc_server).

%%%_* Code =============================================================
%% test basic mapreduce functionality
test_basic() ->
  krc_test:with_pb(3, fun(Inputs) ->
    [ {B1, K1, _, _, V1}
    , {B2, K2, _, _, V2}
    , {B3, K3, _, _, V3}
    ]     = Inputs,
    ok    = krc:put(krc_server, krc_obj:new(B1, K1, V1)),
    ok    = krc:put(krc_server, krc_obj:new(B2, K2, V2)),
    ok    = krc:put(krc_server, krc_obj:new(B3, K3, V3)),
    Input = [{B1,K1},{B2,K2}],
    Query = [ {map, {qfun, fun simple_map/3}, none, false}
            , {reduce, {qfun, fun simple_reduce/2}, none, true}],
    {ok, [{_N, [Obj1, Obj2]}]} = krc:mapred(krc_server, Input, Query),
    true = lists:member(krc_obj:val(Obj1), [V1, V2]),
    true = lists:member(krc_obj:val(Obj2), [V1, V2])
  end).

%% test ability to resolve a conflict in a mapreduce job
test_conflict() ->
  krc_test:with_pb(2, fun(Inputs) ->
    [ {B, K, _, _, V1}
    , {_, _, _, _, V2}
    ] = Inputs,
    ok = krc:put(?KRC, krc_obj:new(B, K, V1)),
    ok = krc:put(?KRC, krc_obj:new(B, K, V2)),
    Input = [{B,K}],
    Query = [{map, {qfun, fun simple_map/3}, none, false},
             {reduce, {qfun, fun simple_reduce/2}, none, true}],
    {ok, [{_N, [Obj]}]} =
      krc:mapred(?KRC, Input, Query, fun conflict_resolve/2),
    V = conflict_resolve(V1, V2),
    V = krc_obj:val(Obj),
    ok
  end).

conflict_resolve( V1, V2) when V1 > V2 -> V1;
conflict_resolve(_V1, V2)              -> V2.

simple_map(Obj, _KeyData, _Arg) ->
  _ = krc_obj:val(Obj), %assert
  [Obj].

simple_reduce(L, _Arg) ->
  L.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
