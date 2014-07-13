%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Protobuf client.
%%% @copyright 2012 Klarna AB
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%
%%%   Copyright 2011-2013 Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%_* Module declaration ===============================================
-module(krc_pb_client).
-behaviour(krc_riak_client).

%%%_* Exports ==========================================================
-export([ delete/5
        , get/5
        , get_index/5
        , put/4
        , start_link/3
        , search/4
        ]).

%%%_* Includes =========================================================
-include_lib("tulib/include/prelude.hrl").

%%%_* Code =============================================================
delete(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:delete(
      Pid, krc_obj:encode(Bucket), krc_obj:encode(Key), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

get(Pid, {<<"crdt", _/binary>>, _} = Bucket, Key, Options, _Timeout) ->
  case
    riakc_pb_socket:fetch_type(Pid, Bucket, Key, Options)
  of
    {ok, Val} -> {ok, krc_obj:new(Bucket, Key, [Val])};
    {error, _} = Err -> Err
  end;
get(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:get(
      Pid, Bucket, Key, Options, Timeout)
  of
    {ok, Obj}        -> {ok, krc_obj:from_riakc_obj(Obj)};
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Key, Timeout) ->
  case
    riakc_pb_socket:get_index(Pid,
                              krc_obj:encode(Bucket),
                              krc_obj:encode_idx(Index),
                              krc_obj:encode_idx_key(Key),
                              Timeout,
                              infinity) %gen_server call
  of
    {ok, Keys}       -> {ok, [krc_obj:decode(K) || K <- Keys]};
    {error, _} = Err -> Err
  end.

put(Pid, Obj, Options, Timeout) ->
  put(Pid, Obj, Options, Timeout, krc_obj:val(Obj)).

put(Pid, Obj, _Options, _Timeout, {update_type, Fun})  ->
  {Type, Bucket}  = krc_obj:bucket(Obj),

  %% Key in the krc_obj is a list at the moment, would prefer it to be a binary, but it breaks soapbox and it's
  %% more work than it's worth to change it
  Key = wf:to_binary(krc_obj:key(Obj)),
  Fun_ = riakc_map:to_op(Fun),

  case
    riakc_pb_socket:update_type(Pid, {wf:to_binary(Type), wf:to_binary(Bucket)}, Key, Fun_, [])
  of
    %ok                  -> ok;
    {ok, _ReturnVal}     -> ok; %{ok, ReturnVal};
    {ok, _Key, _DataType} -> ok; %{ok, {Key, DataType}};
    {error, _} = Err    -> Err
  end;
put(Pid, Obj, _Options, _Timeout, {modify_type, Fun}) ->
  Bucket = krc_obj:bucket(Obj),
  %% Key in the krc_obj is a list at the moment, would prefer it to be a binary, but it breaks soapbox and it's
  %% more work than it's worth to change it
  Key = wf:to_binary(krc_obj:key(Obj)),
  case
    riakc_pb_socket:modify_type(Pid, Fun, Bucket, Key, [create, return_body])
  of
    ok               -> ok;
    {ok, DataType}  -> io:format("{ok, ~p}~n", [DataType]),ok;
    {error, _} = Err -> Err
  end;
put(Pid, Obj, Options, Timeout, _) ->
  case
    riakc_pb_socket:put(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.
search(Pid, Idx, Query, _Timeout) ->
    case
        riakc_pb_socket:search(Pid, Idx, Query)
    of
        {ok, SearchResults}     -> {ok, SearchResults};
        {error, Err}            -> Err
    end.


start_link(IP, Port, Options) ->
  {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
  pong      = riakc_pb_socket:ping(Pid), %ensure server actually reachable
  {ok, Pid}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
