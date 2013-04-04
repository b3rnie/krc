%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Protobuf client.
%%%
%%% Copyright 2013 Kivra AB
%%% Copyright 2011-2013 Klarna AB
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
-module(krc_pb_client).
-behaviour(krc_riak_client).

%%%_* Exports ==========================================================
-export([ delete/5
        , get/5
	, get_bucket/3
        , get_index/5
        , get_index/6
	, mapred/4
        , put/4
	, set_bucket/4
        , start_link/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
delete(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:delete(Pid,
			   krc_obj:encode_key(Bucket),
			   krc_obj:encode_key(Key),
			   Options,
			   Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

get(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:get(Pid,
			krc_obj:encode_key(Bucket),
			krc_obj:encode_key(Key),
			Options,
			Timeout)
  of
    {ok, Obj}        -> {ok, krc_obj:from_riakc_obj(Obj)};
    {error, _} = Err -> Err
  end.

get_bucket(Pid, Bucket, Timeout) ->
  case
    riakc_pb_socket:get_bucket(Pid,
			       krc_obj:encode_key(Bucket),
			       Timeout) of
    {ok, Props}      -> {ok, krc_bucket_properties:decode(Props)};
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Key, Timeout) ->
  {Idx, IdxKey} = krc_obj:encode_index({Index, Key}),
  case
    riakc_pb_socket:get_index(Pid,
                              krc_obj:encode_key(Bucket),
                              Idx,
                              IdxKey,
                              Timeout,
                              infinity) %gen_server call
  of
    {ok, Keys}       -> {ok, [krc_obj:decode_key(K) || K <- Keys]};
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Lower, Upper, Timeout) ->
  {Idx, LowerKey} = krc_obj:encode_index({Index, Lower}),
  {Idx, UpperKey} = krc_obj:encode_index({Index, Upper}),
  case
    riakc_pb_socket:get_index(Pid,
                              krc_obj:encode_key(Bucket),
                              Idx,
                              LowerKey,
                              UpperKey,
                              Timeout,
                              infinity) %gen_server call
  of
    {ok, Keys}       -> {ok, [krc_obj:decode_key(K) || K <- Keys]};
    {error, _} = Err -> Err
  end.

mapred(Pid, Input0, Query0, Timeout) ->
  Input = mapred_input(Input0),
  Query = mapred_query(Query0),

  case riakc_ob_socket:mapred(Pid,
			      Input,
			      Query,
			      Timeout, infinity) %gen_server call
  of
    {ok, _} = Res    -> Res;
    {error, _} = Err -> Err
  end.

put(Pid, Obj, Options, Timeout) ->
  case
    riakc_pb_socket:put(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

set_bucket(Pid, Bucket, Props, Timeout) ->
  case
    riakc_pb_socket:set_bucket(Pid,
			       krc_obj:encode_key(Bucket),
			       Props,
			       Timeout) of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

start_link(IP, Port, Options) ->
  {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
  pong      = riakc_pb_socket:ping(Pid), %ensure server actually reachable
  {ok, Pid}.


%%%_ * Mapred transformation -------------------------------------------
%% krc uses krc_obj's and encoded buckets and keys.
mapred_input(Input) ->
  lists:map(
    fun({{B, K}, D}) -> {{krc_obj:encode(B), krc_obj:encode(K)}, D};
       ({B, K})      -> {krc_obj:encode(B), krc_obj:encode(K)}
    end, Input).

mapred_query(Query) ->
  lists:map(fun({map, FunTerm, Arg, Keep}) ->
		{map, {qfun, rewrite_funterm(FunTerm)}, Arg, Keep};
	       ({reduce, FunTerm, Arg, Keep}) ->
		{reduce, FunTerm, Arg, Keep}
	    end, Query).

rewrite_funterm({modfun, M, F}) ->
  fun(Obj, KeyData, Arg) ->
      M:F(krc_obj:from_riakc_obj(Obj), KeyData, Arg)
  end;
rewrite_funterm({qfun, F}) ->
  fun(Obj, KeyData, Arg) ->
      F(krc_obj:from_riakc_obj(Obj), KeyData, Arg)
  end.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
