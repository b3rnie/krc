%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Mapreduce
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
-module(krc_mapred).

%%%_* Exports ==========================================================
-export([ encode_input/1
        , encode_query/2
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%% @doc krc uses encoded bucket/key.
encode_input(Input) ->
  lists:map(
    fun({{B, K}, D}) -> {{krc_obj:encode_key(B), krc_obj:encode_key(K)}, D};
       ({B, K})      -> {krc_obj:encode_key(B),  krc_obj:encode_key(K)}
    end, Input).

%% @doc krc uses krc_obj.
encode_query(Query, Strat) ->
  lists:map(
    fun({map, FunTerm, Arg, Keep}) ->
        {map, {qfun, rewrite_fun(FunTerm, Strat)}, Arg, Keep};
       ({reduce, _FunTerm, _Arg, _Keep} = Red) -> Red
    end, Query).

%%%_ * Internals -------------------------------------------------------
%% @doc patch mapreduce job to resolve conflicts and present krc_objs.
rewrite_fun(FunTerm, Strat) ->
  fun(Obj, KeyData, Arg) ->
      KrcObj0 = krc_obj:from_riak_obj(Obj),
      KrcObj  = resolve(KrcObj0, Strat),
      case FunTerm of
        {modfun, M, F} -> M:F(KrcObj, KeyData, Arg);
        {qfun, F}      -> F(KrcObj, KeyData, Arg)
      end
  end.

resolve(KrcObj, Strat) when is_atom(Strat) ->
  resolve(KrcObj, krc_resolver:compose(Strat:lookup(krc_obj:bucket(KrcObj))));
resolve(KrcObj, Strat) when is_function(Strat) ->
  ?unlift(krc_obj:resolve(KrcObj, Strat)).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
