%% Copyright 2011 Brian Lee Yung Rowe
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(bunny_farm).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform,lager_transform}]).
-export([open/1, open/2, open/3, close/1, close/2]).
-export([declare_exchange/1, declare_exchange/2,
   declare_queue/1, declare_queue/2, declare_queue/3, bind/3]).
-export([consume/1, consume/2, publish/3, rpc/3, rpc/4, respond/3]).
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2]).
-ifdef(TEST).
-compile(export_all).
-endif.

-record(bunny_state, {conn, chan, conn_mon, chan_mon, confirms,
		      acks = 0, nacks = 0, open_args}).

%% Convenience function for opening a connection for publishing
%% messages. The routing key can be included but if it is not,
%% then the connection can be re-used for multiple routing keys
%% on the same exchange.
%% Example
%%   BusHandle = bunny_farm:open(<<"my.exchange">>),
%%   bunny_farm:publish(Message, K,BusHandle),
open(MaybeTuple) ->
  open(MaybeTuple, false).

open(MaybeTuple, Bool) when is_boolean(Bool) ->
  open1(MaybeTuple, Bool);
open(MaybeTuple, {_,_} = TBC) ->
  open1(MaybeTuple, TBC);
open(MaybeX, MaybeK) ->
  open(MaybeX, MaybeK, false).

open1(MaybeTuple, TBC) ->
  {X,XO} = resolve_options(exchange, MaybeTuple),
  BusHandle = open_it(#bus_handle{exchange=X, options=XO}, TBC),
  bunny_farm:declare_exchange(BusHandle),
  BusHandle.

%% Convenience function to open and declare all intermediate objects. This
%% is the typical pattern for consuming messages from a topic exchange.
%% @returns bus_handle
%% Example
%%   BusHandle = bunny_farm:open(X, K),
%%   bunny_farm:consume(BusHandle),

open(MaybeX, MaybeK, Bool) when is_boolean(Bool) ->
  {X,XO} = resolve_options(exchange, MaybeX),
  {K,KO} = resolve_options(queue, MaybeK),
  BusHandle = open_it(#bus_handle{exchange=X, options=XO}, Bool),
  declare_exchange(BusHandle),
  case X of
    <<"">> -> Q = declare_queue(K, BusHandle, KO);
    _ -> Q = declare_queue(BusHandle, KO)
  end,
  bind(Q, K, BusHandle).


close(#bus_handle{conn=Connection}) ->
  gen_server:cast(Connection, {close, <<"">>}).
close(#bus_handle{conn=Connection}, Tag) ->
  gen_server:cast(Connection, {close, Tag}).


consume(#bus_handle{}=BusHandle) ->
  consume(BusHandle, []).
consume(#bus_handle{queue=Q,conn=Conn}, Options) when is_list(Options) ->
  AllOptions = [{queue,Q}, {no_ack,true}] ++ Options,
  BasicConsume = farm_tools:to_basic_consume(AllOptions),
  lager:debug("Sending subscription request:~n  ~p", [BasicConsume]),
  gen_server:call(Conn, {consume, BasicConsume, self()}).


publish(#message{payload=Payload, props=Props}, K,
        #bus_handle{exchange=X, conn=Conn, options=Options}) ->
  MimeType = case farm_tools:content_type(Props) of
	       undefined -> farm_tools:encoding(Options);
	       M -> M
  end,
  EncPayload = farm_tools:encode_payload(MimeType, Payload),
  ContentType = {content_type,MimeType},
  AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
  AMsg = #amqp_msg{payload=EncPayload, props=AProps},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
  gen_server:cast(Conn, {publish, BasicPublish, AMsg});
publish(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, RoutingKey, BusHandle).


rpc(#message{payload=Payload, props=Props}, K,
    #bus_handle{exchange=X, conn=Conn, options=Options}) ->
  MimeType = case farm_tools:content_type(Props) of
	       undefined -> proplists:get_value(encoding, Options);
	       M -> M
	     end,
  ContentType = {content_type,MimeType},
  AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType,Payload),
                   props=AProps},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
  gen_server:cast(Conn, {rpc, BasicPublish, AMsg}).
rpc(Payload, ReplyTo, K, BusHandle) ->
  Props = [{reply_to,ReplyTo}, {correlation_id,ReplyTo}],
  rpc(#message{payload=Payload, props=Props}, K, BusHandle).


%% This is used to send the response of an RPC. The primary difference
%% between this and publish is that the data is retained as an erlang
%% binary.
respond(#message{payload=Payload, props=Props}, RoutingKey,
        #bus_handle{exchange=X,conn=Conn}) ->
  MimeType = farm_tools:content_type(Props),
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType,Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey},
  error_logger:info_msg("Responding to ~p~n", [BasicPublish]),
  gen_server:cast(Conn, {rpc, BasicPublish, AMsg});
respond(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  respond(#message{payload=Payload}, RoutingKey, BusHandle).


%% This is special handling for the default exchange. This exchange cannot be
%% explicitly declared so it just returns.
declare_exchange(#bus_handle{exchange= <<"">>}) -> ok;
%% Type - The exchange type (e.g. <<"topic">>)
declare_exchange(#bus_handle{exchange=Key, conn=Conn, options=Options}) ->
  AllOptions = lists:merge([{exchange,Key}], Options),
  ExchDeclare = farm_tools:to_exchange_declare(AllOptions),
  lager:debug("Declaring exchange: ~p", [ExchDeclare]),
  gen_server:call(Conn, {declare_exchange, ExchDeclare}).
declare_exchange(<<"">>, #bus_handle{}) -> ok;
declare_exchange(Key, #bus_handle{}=BusHandle) ->
  declare_exchange(BusHandle#bus_handle{exchange=Key}).


%% http://www.rabbitmq.com/amqp-0-9-1-quickref.html
%% Use configured options for the queue. Since no routing key is specified,
%% attempt to read options for the routing key <<"">>.
declare_queue(#bus_handle{}=BusHandle) ->
  declare_queue(BusHandle, queue_options(<<"">>)).
%% Options - Tuple list of k,v options
declare_queue(#bus_handle{}=BusHandle, Options) when is_list(Options) ->
  declare_queue(<<"">>, BusHandle, Options).
declare_queue(Key, #bus_handle{conn=Conn}, Options) ->
  AllOptions = lists:merge([{queue,Key}], Options),
  QueueDeclare = farm_tools:to_queue_declare(AllOptions),
  gen_server:call(Conn, {declare_queue, QueueDeclare}).


bind(Q, _BindKey, #bus_handle{exchange= <<"">>}=BusHandle) ->
  BusHandle#bus_handle{queue=Q};
bind(Q, BindKey, #bus_handle{exchange=X, conn=Conn}=BusHandle) ->
  QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
  gen_server:call(Conn, {bind, QueueBind}),
  BusHandle#bus_handle{queue=Q}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_it(#bus_handle{}=BusHandle, TBCBool) ->
  Keys = [amqp_username, amqp_password,amqp_virtual_host],
  {H,R} = get_server(),
  lager:debug("Opening connection to ~p:~p", [H,R]),
  lager:debug("Calling pid is ~p", [self()]),
  lager:debug("Calling application is ~p", [application:get_application()]),
  [U,P,V] = lists:map(fun get_env/1, Keys),
  Params = #amqp_params_network{username=U, password=P, virtual_host=V,
				host=H, port=R},
  {ok,Connection} = gen_server:start_link(?MODULE, [Params, TBCBool], []),
  BusHandle#bus_handle{conn=Connection}.

default(Key) ->
    Defaults = [{amqp_username, <<"guest">>},
                {amqp_password, <<"guest">>},
                {amqp_virtual_host, <<"/">>},
                {amqp_servers, []}, % Format is {host,port}
                {amqp_host, "localhost"},
                {amqp_port, 5672},
                {amqp_encoding, <<"application/x-erlang">>},
                {amqp_exchanges, []},
                {amqp_queues, []}],

    %% Note(superbobry): try fetching the value from 'bunny_farm'
    %% environment first, this might be useful for example when all
    %% applications use a single RabbitMQ instance.
    case application:get_env(bunny_farm, Key) of
        {ok, Value} -> Value;
        undefined   ->
            proplists:get_value(Key, Defaults)
    end.

get_env(Key) ->
  Default = default(Key),
  case application:get_env(Key) of
    undefined -> Default;
    {ok, H} -> H
  end.

%% If amqp_servers is defined, use that. Otherwise fall back to amqp_host and
%% amqp_port
get_server() ->
  case get_env(amqp_servers) of
    [] ->
      {get_env(amqp_host), get_env(amqp_port)};
    Servers ->
      Idx = random:uniform(length(Servers)),
      lists:nth(Idx, Servers)
  end.

%% Define defaults that override rabbitmq defaults
exchange_defaults() ->
  Encoding = get_env(amqp_encoding),
  lists:sort([ {encoding, Encoding}, {type,<<"topic">>} ]).

%% Get the proplist for a given channel
exchange_options(X) ->
  Channels = get_env(amqp_exchanges),
  ChannelList = [ List || {K, List} <- Channels, K == X ],
  case ChannelList of
    [] -> [];
    [Channel] -> lists:sort(Channel)
  end.

%% Define defaults that override rabbitmq defaults
queue_defaults() ->
  lists:sort([ {exclusive,true} ]).

queue_options(X) ->
  Channels = get_env(amqp_queues),
  ChannelList = [ List || {K, List} <- Channels, K == X ],
  case ChannelList of
    [] -> [];
    [Channel] -> lists:sort(Channel)
  end.

%% Decouple the exchange and options. If no options exist, then use defaults.
resolve_options(exchange, MaybeTuple) ->
  Defaults = exchange_defaults(),
  case MaybeTuple of
    {X,O} -> Os = lists:merge([lists:sort(O),exchange_options(X),Defaults]);
    X -> Os = lists:merge([exchange_options(X),Defaults])
  end,
  {X,Os};

resolve_options(queue, MaybeTuple) ->
  Defaults = queue_defaults(),
  case MaybeTuple of
    {K,O} -> Os = lists:merge([lists:sort(O),queue_options(K),Defaults]);
    K -> Os = lists:merge(queue_options(K),Defaults)
  end,
  {K,Os}.

init([Params, TBCBool]) ->
  {ok,Connection} = amqp_connection:start(Params),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  TBC =
    case TBCBool of
      false -> false;
      true ->
	#'confirm.select_ok'{} =
	  amqp_channel:call(Channel, #'confirm.select'{}),
	ok = amqp_channel:register_confirm_handler(Channel, self()),
	ets:new(?MODULE, [ordered_set,public])
    end,
  ConnMon = erlang:monitor(process, Connection),
  ChanMon = erlang:monitor(process, Channel),
  {ok, #bunny_state{conn      = Connection,
		    chan      = Channel,
		    conn_mon  = ConnMon,
		    chan_mon  = ChanMon,
		    confirms  = TBC,
		    open_args = Params}}.


handle_call({consume, BasicConsume, Pid}, _From,
	    #bunny_state{chan = Channel} = State) ->
  Reply = amqp_channel:subscribe(Channel, BasicConsume, Pid),
  {reply, Reply, State};
handle_call({declare_exchange, ExchDeclare}, _From,
	    #bunny_state{chan = Channel} = State) ->
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchDeclare),
  {reply, ok, State};
handle_call({declare_queue, QueueDeclare}, _From,
	    #bunny_state{chan = Channel} = State) ->
  #'queue.declare_ok'{queue=Q} = amqp_channel:call(Channel, QueueDeclare),
  {reply, Q, State};
handle_call({bind, QueueBind}, _From,
	    #bunny_state{chan = Channel} = State) ->
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
  {reply, ok, State}.


handle_cast({publish, BasicPublish, AMsg},
	    #bunny_state{chan     = Channel,
			 confirms = TBC} = State) ->
  case TBC of
    false -> ok;
    _ ->
      SeqNo = amqp_channel:next_publish_seqno(Channel),
      ets:insert(TBC, {SeqNo, BasicPublish, AMsg})
  end,
  amqp_channel:cast(Channel, BasicPublish, AMsg),
  {noreply, State};
handle_cast({rpc, BasicPublish, AMsg},
	    #bunny_state{chan = Channel} = State) ->
  amqp_channel:cast(Channel, BasicPublish, AMsg),
  {noreply, State};
handle_cast({close, ChannelTag},
	    #bunny_state{conn     = Connection,
			 chan     = Channel,
			 conn_mon = ConnMon,
			 chan_mon = ChanMon} = State) ->
  case ChannelTag of
    <<"">> -> ok;
    Tag ->
      #'basic.cancel_ok'{} =
	amqp_channel:call(Channel, #'basic.cancel'{consumer_tag=Tag})
  end,
  erlang:monitor(ConnMon),
  erlang:monitor(ChanMon),
  amqp_connection:close(Connection),
  amqp_channel:close(Channel),
  {stop, State}.

handle_info(#'basic.ack'{delivery_tag = SeqNo, multiple = false},
	    #bunny_state{confirms = Ets, acks = C} = State) ->
  ets:delete(Ets, SeqNo),
  {noreply, State#bunny_state{acks = C + 1}};
handle_info(#'basic.ack'{delivery_tag = SeqNo, multiple = true},
	    #bunny_state{confirms = Ets, acks = C} = State) ->
  C1 = delete_old(Ets, SeqNo, 0),
  {noreply, State#bunny_state{acks = C + C1}};
handle_info(#'basic.nack'{delivery_tag = OldSeqNo, multiple = false  },
	    %  requeue = true},
	    #bunny_state{confirms = Ets, chan = Channel, nacks = C}
	    = State) ->
  case ets:lookup(Ets,OldSeqNo) of
    {OldSeqNo, BasicPublish, AMsg} ->
      p_resend(Ets, Channel, OldSeqNo, BasicPublish, AMsg);
    _ -> ok
  end,
  {noreply, State#bunny_state{nacks = C + 1}};

handle_info({'DOWN', Ref, process, Pid, Reason},
	    #bunny_state{conn      = Pid,
			 conn_mon  = Ref,
			 open_args = Params,
			 confirms  = TBC}=State) ->
  % Connection crashed, so channel is automatically gone too
  lager:error("AMQP Connection exit: ~p",[Reason]),
  {ok,Connection} = amqp_connection:start(Params),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  case TBC of
    false -> ok;
    _ ->
      #'confirm.select_ok'{} =
	amqp_channel:call(Channel, #'confirm.select'{}),
      ok = amqp_channel:register_confirm_handler(Channel, self()),
      ets:foldl(fun({OldSeqNo, BasicPublish, AMsg}, ok) ->
		    p_resend(TBC, Channel, OldSeqNo, BasicPublish, AMsg)
		end, ok, TBC)
  end,
  ConnMon = erlang:monitor(process, Connection),
  ChanMon = erlang:monitor(process, Channel),
  {noreply, State#bunny_state{conn     = Connection,
			      chan     = Channel,
			      conn_mon = ConnMon,
			      chan_mon = ChanMon}};
handle_info({'DOWN', Ref, process, Pid, Reason},
	    #bunny_state{conn     = Connection,
			 chan     = Pid,
			 chan_mon = Ref,
			 confirms = TBC}=State) ->
  % Channel crashed, so try to use the same connection
  lager:error("AMQP Channel exit: ~p",[Reason]),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  case TBC of
    false -> ok;
    _ ->
      #'confirm.select_ok'{} =
	amqp_channel:call(Channel, #'confirm.select'{}),
      ok = amqp_channel:register_confirm_handler(Channel, self()),
      ets:foldl(fun({OldSeqNo, BasicPublish, AMsg}, ok) ->
		    p_resend(TBC, Channel, OldSeqNo, BasicPublish, AMsg)
		end, ok, TBC)
  end,
  ChanMon = erlang:monitor(process, Channel),
  {noreply, State#bunny_state{chan     = Channel,
			      chan_mon = ChanMon}};   
handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_,_) ->
  ok.

delete_old(_Ets, '$end_of_table', C) -> C;
delete_old(Ets, SeqNo, C) ->
  ets:delete(Ets, SeqNo),
  delete_old(Ets, ets:prev(Ets, SeqNo), C+1).

p_resend(Ets, Channel, OldSeqNo, BasicPublish, AMsg) ->
  ets:delete(Ets, OldSeqNo),
  NewSeqNo = amqp_channel:next_publish_seqno(Channel),
  amqp_channel:cast(Channel, BasicPublish, AMsg),
  ets:insert(Ets, {NewSeqNo, BasicPublish, AMsg}),
  ok.
