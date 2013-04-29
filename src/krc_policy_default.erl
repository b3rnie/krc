-module(krc_policy_default).
-behaviour(krc_policy).
-export([lookup/1]).

lookup(_Bucket) ->
  [ krc_resolver_defaulty
  ].
