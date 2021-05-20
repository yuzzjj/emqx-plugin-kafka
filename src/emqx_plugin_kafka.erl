%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-include("emqx.hrl").
-include_lib("brod/include/brod_int.hrl").

-define(APP, emqx_plugin_kafka).

-export([ load/1
        , unload/0
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connect/3
        , on_client_connack/4
        , on_client_connected/3
        , on_client_disconnected/4
        , on_client_authenticate/3
        , on_client_check_acl/5
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_resumed/3
        , on_session_discarded/3
        , on_session_takeovered/3
        , on_session_terminated/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        , on_message_dropped/4
        ]).

%% Called when the plugin application start
load(Env) ->
    brod_init([Env]),
    emqx:hook('client.connect',      {?MODULE, on_client_connect, [Env]}),
    emqx:hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
    emqx:hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
    emqx:hook('client.check_acl',    {?MODULE, on_client_check_acl, [Env]}),
    emqx:hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
    emqx:hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
    emqx:hook('session.created',     {?MODULE, on_session_created, [Env]}),
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('session.resumed',     {?MODULE, on_session_resumed, [Env]}),
    emqx:hook('session.discarded',   {?MODULE, on_session_discarded, [Env]}),
    emqx:hook('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}),
    emqx:hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    emqx:hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    emqx:hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    emqx:hook('message.acked',       {?MODULE, on_message_acked, [Env]}),
    emqx:hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}).

brod_init(_Env) ->
    % broker 代理服务器的地址
    % BootstrapBrokers =  "a01-r26-i139-170-06dbver.jd.local:9092,a01-r26-i139-157-06dbvkl.jd.local:9092,a01-r26-i139-171-06dbxfh.jd.local:9092",
    {ok, BootstrapBrokers} = get_bootstrap_brokers(),
    io:format(">>>>>>>>>kafka brokers:~p <<<<<<<<<< ~n", [BootstrapBrokers]),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(brod),
    io:format("<<<<<<<<<<< -- after ensure_all_started  --  >>>>>>>>>>"),
    % socket error recovery
    ClientConfig =
        [
           {reconnect_cool_down_seconds, 10},
           %% avoid api version query in older version brokers. needed with kafka 0.9.x or earlier.
           % {query_api_version, false},

           %% Auto start producer with default producer config
           {auto_start_producers, true},
           %%
           {default_producer_config, []},

           %% disallow
           {allow_topic_auto_creation, true}
        ],

    ok = brod:start_client(BootstrapBrokers, brod_client_1, ClientConfig),
    % Start a Producer on Demand
    %ok = brod:start_producer(brod_client_1, DpTopic, _ProducerConfig = []),
    %ok = brod:start_producer(brod_client_1, DsTopic, _ProducerConfig = []),
    io:format("---><Init brod kafka success><---~n").

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
    io:format("Client(~s) connect, ConnInfo: ~p, Props: ~p~n",
              [ClientId, ConnInfo, Props]),
    {ok, Props}.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
    io:format("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
              [ClientId, ConnInfo, Rc, Props]),
    {ok, Props}.

on_client_connected(ClientInfo=#{
        clientid := ClientId,
        username := Username}, ConnInfo, _Env) ->
    Json = jiffy:encode({[
        {type, <<"connected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, a2b(node())},
        {ts, erlang:system_time(millisecond)}
    ]}),
    io:format("<<kafka json>>Client(~s) connected, Json: ~s~n", [ClientId, Json]),
    ok = produce_status(ClientId, Json),
    io:format("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
              [ClientId, ClientInfo, ConnInfo]).

on_client_disconnected(ClientInfo = #{
        clientid := ClientId,
        username := Username}, ReasonCode, ConnInfo, _Env) ->
    io:format("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
              [ClientId, ReasonCode, ClientInfo, ConnInfo]),
    Json = jiffy:encode({[
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, a2b(node())},
        {reason, a2b(ReasonCode)},
        {ts, erlang:system_time(millisecond)}
    ]}),
    io:format("<<kafka json>>Client(~s) disconnected, Json: ~s~n", [ClientId, Json]),
    ok = produce_status(ClientId, Json),
    ok.

on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
    io:format("Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
    {ok, Result}.

on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
    io:format("Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
              [ClientId, PubSub, Topic, Result]),
    {ok, Result}.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
    io:format("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
              [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #{
           clientid := ClientId,
           username := Username,
           from := From,
           pktid := _PkgId,
           qos := Qos,
           retain := Retain,
           dup := Dup,
           topic := Topic,
           payload := Payload,
           ts := Ts}, _Env) ->
    io:format("Publish ~s~n", [emqx_message:format(Message)]),
    Json = jiffy:encode({[
        {type, <<"published">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {payload, Payload},
        {qos, Qos},
        {dup, Dup},
        {retain, Retain},
        {cluster_node, a2b(node())},
        {ts, Ts}
    ]}),
    io:format("<<kafka json>>Client(~s) publish, Json: ~s~n", [ClientId, Json]),
    ok = produce_points(ClientId, Json),
    {ok, Message}.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
    io:format("Message dropped by node ~s due to ~s: ~s~n",
              [Node, Reason, emqx_message:format(Message)]).

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message delivered to client(~s): ~s~n",
              [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message acked by client(~s): ~s~n",
              [ClientId, emqx_message:format(Message)]).

produce_points(ClientId, Json) ->
    Topic = get_points_topic(),
    produce(Topic, ClientId, Json),
    ok.

produce_status(ClientId, Json) ->
    Topic = get_status_topic(),
    produce(Topic, ClientId, Json),
    ok.

produce(TopicInfo, ClientId, Json) ->
    case TopicInfo of
        {ok, Topic, custom, _}->
            brod_produce(Topic, hash, ClientId, Json);
        {ok, Topic, _, _} ->
            brod_produce(Topic, random, ClientId, Json)
    end.

brod_produce(Topic, Partitioner, ClientId, Json) ->
    io:format("<<MSG>> Topic: ~p, Partitioner: ~p, ClientId:~s JSON: ~p", [Topic, Partitioner, ClientId, Json]),
    %{ok, CallRef} = brod:produce(brod_client_1, Topic, Partitioner, ClientId, list_to_binary(Json)),
    %receive
    %    #brod_produce_reply{call_ref = CallRef, result = brod_produce_req_acked} -> ok
    %after 5000 ->
    %    lager:error("Produce message to ~p for ~p timeout.",[Topic, ClientId])
    %end,
    ok.

a2b(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8);
a2b(A) -> A.

%% 从配置中获取当前Kafka的初始broker配置

get_bootstrap_brokers() ->
    application:get_env(?APP, bootstrap_brokers).

get_config_prop_list() ->
    application:get_env(?APP, config).

get_instrument_config() ->
    {ok, Values} = get_config_prop_list(),
    Instrument = proplists:get_value(instrument, Values),
    {ok, Instrument}.

%% 从配置中获取设备数据流主题Points的配置
get_points_topic() ->
    {ok, Values} = application:get_env(?APP, points),
    get_topic(Values).

%% 从配置中获取设备状态流主题Status的配置
get_status_topic() ->
    {ok, Values} = application:get_env(?APP, status),
    get_topic(Values).

get_topic(Values) ->
    Topic = proplists:get_value(topic, Values),
    PartitionStrategy = proplists:get_value(partition_strategy, Values),
    PartitionWorkers = proplists:get_value(partition_workers, Values),
    {ok, Topic, PartitionStrategy, PartitionWorkers}.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connect',      {?MODULE, on_client_connect}),
    emqx:unhook('client.connack',      {?MODULE, on_client_connack}),
    emqx:unhook('client.connected',    {?MODULE, on_client_connected}),
    emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    emqx:unhook('client.check_acl',    {?MODULE, on_client_check_acl}),
    emqx:unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
    emqx:unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    emqx:unhook('session.created',     {?MODULE, on_session_created}),
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('session.resumed',     {?MODULE, on_session_resumed}),
    emqx:unhook('session.discarded',   {?MODULE, on_session_discarded}),
    emqx:unhook('session.takeovered',  {?MODULE, on_session_takeovered}),
    emqx:unhook('session.terminated',  {?MODULE, on_session_terminated}),
    emqx:unhook('message.publish',     {?MODULE, on_message_publish}),
    emqx:unhook('message.delivered',   {?MODULE, on_message_delivered}),
    emqx:unhook('message.acked',       {?MODULE, on_message_acked}),
    emqx:unhook('message.dropped',     {?MODULE, on_message_dropped}),
    
    % It is ok to leave brod application there.
    brod:stop_client(brod_client_1),
    io:format("Finished all unload works.").
