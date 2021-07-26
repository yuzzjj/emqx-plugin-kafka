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
           {allow_topic_auto_creation, true},
           {client_id, <<"imart.jd.com">>},
           %% using sasl 
           %% {ssl, ssl_options()},
           {sasl, {plain, "Pf15ceb26", "v3hwYJTBgytcQFek"}}
        ],
    %%brod_client_1 = <<"imart.jd.com">>,
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
        username := Username,
        peerhost := PeerHost}, ConnInfo, _Env) ->
    F = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,
    Json = jiffy:encode({[
        {type, <<"connected">>},
        {client_id, F(ClientId)},
        {username, F(Username)},
        {peerhost, ntoa(PeerHost)},
        {cluster_node, a2b(node())},
        {timestamp, erlang:system_time(millisecond)}
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

    F1 = fun(R) -> case is_atom(R) of true -> atom_to_binary(R, utf8); _ -> <<"normal">> end end,
    F2 = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,
    %IP =  io_lib:format("~B.~B.~B.~B",[B1, B2, B3, B4]),

    Json = jiffy:encode({[
        {type, <<"disconnected">>},
        {client_id, F2(ClientId)},
        {username, F2(Username)},
        {cluster_node, a2b(node())},
        {reason, a2b(ReasonCode)},
        {timestamp, erlang:system_time(millisecond)}
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

on_message_publish(Message = #message{
           id = <<I1:64, I2:48, I3:16>> = _MsgId,
           headers = #{peerhost := PeerHost, username := Username},
           from = ClientId,
           qos = QoS,
           flags = #{dup := Dup, retain := Retain},
           topic = Topic,
           payload = Payload,
           timestamp = Ts}, _Env) ->
    %%io:format("Publish ~s~n", [emqx_message:format(Message)]),

    F1 = fun(X) -> case X of  true ->1; _ -> 0 end end,
    F2 = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,

    Json = jiffy:encode({[
        {type, <<"published">>},
        {id, I1 + I2 + I3},
        {client_id, ClientId},
        {peerhost, ntoa(PeerHost)},
        {username, F2(Username)},
        {topic, Topic},
        {payload, jiffy:encode({encode_value(Payload)})},
        {qos, QoS},
        {dup, F1(Dup)},
        {retain, F1(Retain)},
        {cluster_node, a2b(node())},
        {timestamp, Ts}
    ]}),
    %%io:format("<<kafka json>>ClientId(~s) publish, Json: ~s~n", [ClientId, Json]),
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
    io:format("<<MSG>> Topic: ~p, Partitioner: ~p, ClientId:~s JSON: ~p~n", [Topic, Partitioner, ClientId, Json]),
    %{ok, CallRef} = brod:produce(brod_client_1, Topic, Partitioner, ClientId, list_to_binary(Json)),
    {ok, CallRef} = brod:produce(brod_client_1, Topic, Partitioner, ClientId, Json),
    receive
        #brod_produce_reply{call_ref = CallRef, result = brod_produce_req_acked} -> ok
    after 5000 ->
        io:format("<<ERROR>>Produce message to ~p for ~p timeout.",[Topic, ClientId])
    end,
    ok.

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).
    
a2b(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8);
a2b(A) -> A.

encode_value(Binary) when is_binary(Binary) -> [{schema, <<"hex">>}, {val, bin_to_hex(Binary)}];
encode_value(List) -> [{schema, <<"origin">>}, {val, List}].


bin_to_hex(B) when is_binary(B) ->
  bin_to_hex(B, <<>>).

-define(H(X), (hex(X)):16).

bin_to_hex(<<>>, Acc) -> Acc;
bin_to_hex(Bin, Acc) when byte_size(Bin) band 7 =:= 0 ->
  bin_to_hex_(Bin, Acc);
bin_to_hex(<<X:8, Rest/binary>>, Acc) ->
  bin_to_hex(Rest, <<Acc/binary, ?H(X)>>).

bin_to_hex_(<<>>, Acc) -> Acc;
bin_to_hex_(<<A:8, B:8, C:8, D:8, E:8, F:8, G:8, H:8, Rest/binary>>, Acc) ->
  bin_to_hex_(
    Rest,
    <<Acc/binary,
      ?H(A), ?H(B), ?H(C), ?H(D), ?H(E), ?H(F), ?H(G), ?H(H)>>).

-compile({inline, [hex/1]}).

hex(X) ->
  element(
    X+1, {16#3030, 16#3031, 16#3032, 16#3033, 16#3034, 16#3035, 16#3036,
          16#3037, 16#3038, 16#3039, 16#3041, 16#3042, 16#3043, 16#3044,
          16#3045, 16#3046, 16#3130, 16#3131, 16#3132, 16#3133, 16#3134,
          16#3135, 16#3136, 16#3137, 16#3138, 16#3139, 16#3141, 16#3142,
          16#3143, 16#3144, 16#3145, 16#3146, 16#3230, 16#3231, 16#3232,
          16#3233, 16#3234, 16#3235, 16#3236, 16#3237, 16#3238, 16#3239,
          16#3241, 16#3242, 16#3243, 16#3244, 16#3245, 16#3246, 16#3330,
          16#3331, 16#3332, 16#3333, 16#3334, 16#3335, 16#3336, 16#3337,
          16#3338, 16#3339, 16#3341, 16#3342, 16#3343, 16#3344, 16#3345,
          16#3346, 16#3430, 16#3431, 16#3432, 16#3433, 16#3434, 16#3435,
          16#3436, 16#3437, 16#3438, 16#3439, 16#3441, 16#3442, 16#3443,
          16#3444, 16#3445, 16#3446, 16#3530, 16#3531, 16#3532, 16#3533,
          16#3534, 16#3535, 16#3536, 16#3537, 16#3538, 16#3539, 16#3541,
          16#3542, 16#3543, 16#3544, 16#3545, 16#3546, 16#3630, 16#3631,
          16#3632, 16#3633, 16#3634, 16#3635, 16#3636, 16#3637, 16#3638,
          16#3639, 16#3641, 16#3642, 16#3643, 16#3644, 16#3645, 16#3646,
          16#3730, 16#3731, 16#3732, 16#3733, 16#3734, 16#3735, 16#3736,
          16#3737, 16#3738, 16#3739, 16#3741, 16#3742, 16#3743, 16#3744,
          16#3745, 16#3746, 16#3830, 16#3831, 16#3832, 16#3833, 16#3834,
          16#3835, 16#3836, 16#3837, 16#3838, 16#3839, 16#3841, 16#3842,
          16#3843, 16#3844, 16#3845, 16#3846, 16#3930, 16#3931, 16#3932,
          16#3933, 16#3934, 16#3935, 16#3936, 16#3937, 16#3938, 16#3939,
          16#3941, 16#3942, 16#3943, 16#3944, 16#3945, 16#3946, 16#4130,
          16#4131, 16#4132, 16#4133, 16#4134, 16#4135, 16#4136, 16#4137,
          16#4138, 16#4139, 16#4141, 16#4142, 16#4143, 16#4144, 16#4145,
          16#4146, 16#4230, 16#4231, 16#4232, 16#4233, 16#4234, 16#4235,
          16#4236, 16#4237, 16#4238, 16#4239, 16#4241, 16#4242, 16#4243,
          16#4244, 16#4245, 16#4246, 16#4330, 16#4331, 16#4332, 16#4333,
          16#4334, 16#4335, 16#4336, 16#4337, 16#4338, 16#4339, 16#4341,
          16#4342, 16#4343, 16#4344, 16#4345, 16#4346, 16#4430, 16#4431,
          16#4432, 16#4433, 16#4434, 16#4435, 16#4436, 16#4437, 16#4438,
          16#4439, 16#4441, 16#4442, 16#4443, 16#4444, 16#4445, 16#4446,
          16#4530, 16#4531, 16#4532, 16#4533, 16#4534, 16#4535, 16#4536,
          16#4537, 16#4538, 16#4539, 16#4541, 16#4542, 16#4543, 16#4544,
          16#4545, 16#4546, 16#4630, 16#4631, 16#4632, 16#4633, 16#4634,
          16#4635, 16#4636, 16#4637, 16#4638, 16#4639, 16#4641, 16#4642,
          16#4643, 16#4644, 16#4645, 16#4646}).


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
