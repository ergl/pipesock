-module(pipesock_conn).

-behaviour(gen_server).

%% API
-export([open/2,
         open/3,
         close/1,
         get_ref/1,
         get_len/1,
         get_self_ip/1,
         send_cb/3,
         send_sync/3,
         send_and_forget/2]).

%% Supervisor callbacks
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% The supervisor of this module
-define(SUPERVISOR, pipesock_conn_sup).

%% Socket options
%% active and deliver_term set options for receiving data
%% packet => raw, since we're doing the framing ourselves
%% (see decode_data/2)
-define(SOCK_OPTS, [binary,
                    {active, once},
                    {deliver, term},
                    {packet, raw}]).

%% Default connection values, can be changed by the client.

%% How many bits in each message are reserved as id portion
-define(ID_BITS, 16).
%% How many milis to wait to flush a buffer after first send
-define(CORK_LEN, 5).
%% Max number of messages waiting on queue before sending
-define(BUFFER_WATERMARK, 500).

%% Use the equivalent of {packet, 4} to frame the messages
-define(FRAME(Data),
    <<(byte_size(Data)):32/big-unsigned-integer, Data/binary>>).

%% @doc Internal state of the connection
-record(state, {
    socket :: gen_tcp:socket(),
    socket_ip :: inet:ip_address(),
    %% TODO(borja): Add option to disable timer on fast networks
    cork_timer = undefined :: timer:tref() | undefined,
    %% How many ms to wait between buffer flushes
    cork_len :: non_neg_integer(),

    %% Reference to use when replying to owners
    %% useful for clients that have more than one connection
    self_ref :: reference(),
    msg_id_len :: non_neg_integer(),
    %% ETS table mapping message ids to callbacks/pids to reply to
    msg_owners = ets:new(msg_owners, [set, private]) :: ets:tid(),

    %% Buffer and ancillary state
    buffer = <<>> :: binary(),
    %% How many messages in the buffer before we flush
    buffer_watermark :: non_neg_integer(),
    buffer_len = 0 :: non_neg_integer(),

    %% Incomplete data coming from socket (since we're framing)
    message_slice = <<>> :: binary()
}).

-type state() :: #state{}.
-type conn_opts() :: map().
-type conn_timeout() :: non_neg_integer() | infinity.

%% @doc Client-facing structure representing a connection
-record(conn_handle, {
    conn_ref :: reference(),
    conn_pid :: pid(),
    conn_ip :: inet:ip_address(),

    %% Mandatory to have this info here to be able to match on the caller side
    %% without validating on the gen_server side.
    id_len :: non_neg_integer()
}).

-opaque conn_handle() :: #conn_handle{}.

-export_type([conn_handle/0, conn_timeout/0]).

%%%===================================================================
%%% Supervision tree
%%%===================================================================

-spec start_link(IP :: inet:ip_address(),
                 Port :: inet:port_number(),
                 Options :: conn_opts()) -> {ok, pid()} | {error, term()}.
start_link(IP, Port, Options) ->
    gen_server:start_link(?MODULE, [IP, Port, Options], []).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get back the unique conn reference
%%
%%      The client might want this for selective receive match
-spec get_ref(conn_handle()) -> reference().
get_ref(#conn_handle{conn_ref=Ref}) ->
    Ref.

%% @doc Get back the message identifier length
%%
%%      The client might want this to wrap messages
-spec get_len(conn_handle()) -> non_neg_integer().
get_len(#conn_handle{id_len=Len}) ->
    Len.

%% @doc Get the local IP of the connection
%%
%%      The client might want this to generate machine-local
%%      identifiers
%%
-spec get_self_ip(conn_handle()) -> inet:ip_address().
get_self_ip(#conn_handle{conn_ip=Ip}) ->
    Ip.

%% @doc Spawn a new TCP connection
%%
%%      Same as pipesock_conn:open(Ip, Port, #{}).
%%
-spec open(Ip :: atom(),
           Port :: inet:port_number()) -> {ok, conn_handle()}
                                        | {error, Reason :: term()}.
open(Ip, Port) ->
    open(Ip, Port, #{}).

%% @doc Spawn a new TCP connection
%%
%%      Valid options are:
%%
%%      id_len => non_neg_integer()
%%          Determines the length (in bits) of the id header for each message
%%          If not supplied, the default is 16. This is mandatory if the caller
%%          wants to receive a message back from the server. Care must be placed
%%          so that the number of clients sharing a single connection doesn't go
%%          over 2^id_len.
%%
%%          The server _must_ return this header intact.
%%
%%      buf_watermark => non_neg_integer()
%%          The max number of messages sitting in the connection buffer.
%%          Once it goes over this number, the connection will automatically
%%          flush it.
%%
%%      cork_len => non_neg_integer()
%%          The max number of milis between buffer flushes.
%%
-spec open(Ip :: atom(),
           Port :: inet:port_number(),
           Options :: conn_opts()) -> {ok, conn_handle()}
                                    | {error, Reason :: term()}.

open(Ip, Port, Options) ->
    IdLen = maps:get(id_len, Options, ?ID_BITS),
    Ret = supervisor:start_child(?SUPERVISOR, [Ip, Port, Options]),
    case Ret of
        {ok, Pid} ->
            {ok, Ref} = get_conn_ref(Pid),
            {ok, LocalIp} = get_conn_ip(Pid),
            {ok, #conn_handle{conn_ref=Ref, conn_pid=Pid,
                              id_len=IdLen, conn_ip=LocalIp}};

        {error, {already_started, ChildPid}} ->
            {ok, Ref} = get_conn_ref(ChildPid),
            {ok, LocalIp} = get_conn_ip(ChildPid),
            {ok, #conn_handle{conn_ref=Ref, conn_pid=ChildPid,
                              id_len=IdLen, conn_ip=LocalIp}};

        Err ->
            Err
    end.

%% @doc Close the TCP connection
-spec close(conn_handle()) -> ok.
close(#conn_handle{conn_pid=Pid}) ->
    gen_server:call(Pid, stop, infinity).

%% @doc Async send
%%
%%      Accepts an optional callback that is fired when a reply
%%      to this message is delivered.
%%
%%      The callback must accept two arguments. The first is the
%%      connection reference (in case one wants to re-use the callback
%%      with different connections), and the reply to this message as
%%      second argument.
%%
-spec send_cb(conn_handle(),
              Msg :: binary(),
              Callback :: fun((reference(), binary()) -> ok)) -> ok.

send_cb(#conn_handle{conn_pid=Pid, id_len=Len},
        Msg, Callback) when is_function(Callback, 2) ->

    <<Id:Len, _/binary>> = Msg,
    gen_server:cast(Pid, {queue, Id, Msg, Callback}).

%% @doc Sync send
%%
%%      Will return when a reply comes or `Timeout` ms pass,
%%      whichever comes first. Set `Timeout` to infinity to
%%      wait forever.
%%
-spec send_sync(conn_handle(),
                Msg :: binary(),
                Timeout :: conn_timeout()) -> {ok, term()}
                                            | {error, timeout}.

send_sync(Conn=#conn_handle{conn_ref=Ref}, Msg, Timeout) ->
    Self = self(),
    send_cb(Conn, Msg, fun(ConnRef, Reply) -> Self ! {ConnRef, Reply} end),
    send_sync_recv(Ref, Timeout).

-spec send_sync_recv(reference(), conn_timeout()) -> {ok, term()}
                                                   | {error, timeout}.
send_sync_recv(Ref, infinity) ->
    receive {Ref, Term} -> {ok, Term} end;

send_sync_recv(Ref, Timeout) ->
    receive {Ref, Term} ->
        {ok, Term}
    after Timeout ->
        {error, timeout}
    end.

%% @doc Async send, where we don't expect a reply
-spec send_and_forget(conn_handle(), Msg :: binary()) -> ok.
send_and_forget(#conn_handle{conn_pid=Pid}, Msg) ->
    gen_server:call(Pid, {queue, Msg}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(term()) -> {ok, state()}| {stop, term()}.
init([IP, Port, Options]) ->
    case gen_tcp:connect(IP, Port, ?SOCK_OPTS) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            {ok, {LocalIP, _LocalPort}} = inet:sockname(Socket),
            Ref = erlang:make_ref(),
            CorkLen = maps:get(cork_len, Options, ?CORK_LEN),
            BufferWatermark = maps:get(buf_watermark, Options, ?BUFFER_WATERMARK),
            {ok, #state{self_ref = Ref,
                        socket = Socket,
                        socket_ip = LocalIP,
                        cork_len = CorkLen,
                        msg_id_len = maps:get(id_len, Options, ?ID_BITS),
                        buffer_watermark = BufferWatermark}}
    end.

%% @doc Get back the unique reference of this connection
handle_call(get_ref, _From, State = #state{self_ref=Ref}) ->
    {reply, {ok, Ref}, State};

handle_call(get_ip, _From, State = #state{socket_ip=Ip}) ->
    {reply, {ok, Ip}, State};

handle_call({queue, Msg}, _From, State) ->
    %% Queue without registering callbacks, useful for "send and forget" messages
    {reply, ok, enqueue_message(Msg, State)};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(E, _From, S) ->
    io:fwrite(standard_error, "unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({queue, Id, Msg, Callback}, State = #state{msg_owners=Owners}) ->
    %% Register callback
    %% Id should be unique. This returns false if `Id` exists
    %% in the owners table, and should crash.
    %% Callers should deal with lost state if this happens.
    true = ets:insert_new(Owners, {Id, Callback}),
    {noreply, enqueue_message(Msg, State)};

handle_cast(E, S) ->
    io:fwrite(standard_error, "unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(flush_buffer, State) ->
    {noreply, maybe_rearm_timer(maybe_cancel_timer(flush_buffer(State)))};

handle_info({tcp, Socket, Data}, State=#state{socket=Socket,
                                              msg_owners=Owners,
                                              msg_id_len=IdLen,
                                              self_ref=OwnRef}) ->

    NewSlice = case decode_data(Data, State#state.message_slice) of
        {more, Rest} ->
            Rest;
        {ok, Msgs, Rest} ->
            ok = process_messages(Msgs, Owners, IdLen, OwnRef),
            Rest
    end,
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{message_slice=NewSlice}};

handle_info({tcp_error, _Socket, Reason}, State) ->
    {stop, Reason, State};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info(E, S) ->
    io:fwrite(standard_error, "unexpected info: ~p~n", [E]),
    {noreply, S}.

terminate(_Reason, #state{socket = Sock, msg_owners = Owners}) ->
    ok = gen_tcp:close(Sock),
    true = ets:delete(Owners),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_conn_ref(Pid :: pid()) -> {ok, reference()}.
get_conn_ref(Pid) ->
    gen_server:call(Pid, get_ref, infinity).

%% @private
-spec get_conn_ip(Pid :: pid()) -> {ok, inet:ip_address()}.
get_conn_ip(Pid) ->
    gen_server:call(Pid, get_ip, infinity).

%% @doc Enqueue a message in the buffer, and update timer and flush state.
-spec enqueue_message(Msg :: binary(), State :: state()) -> state().
enqueue_message(Msg, State = #state{buffer=Buffer,
                                    buffer_len=Len,
                                    buffer_watermark=WM}) ->
    NewLen = Len + 1,
    NewBuffer = <<Buffer/binary, (?FRAME(Msg))/binary>>,
    NewState = State#state{buffer=NewBuffer, buffer_len=NewLen},
    case NewLen =:= WM of
        true ->
            maybe_cancel_timer(flush_buffer(NewState));
        false ->
            maybe_rearm_timer(NewState)
    end.

%% @private
%% @doc Flushes the data buffer through the socket and reset the state
-spec flush_buffer(state()) -> state().
flush_buffer(State = #state{socket=Socket, buffer=Buffer}) ->
    ok = gen_tcp:send(Socket, Buffer),
    State#state{buffer = <<>>, buffer_len = 0}.

%% @private
%% @doc Rearms the timer if it was not yet active
-spec maybe_rearm_timer(state()) -> state().
maybe_rearm_timer(State=#state{cork_timer = undefined, cork_len = Span}) ->
    {ok, TRef} = timer:send_after(Span, flush_buffer),
    State#state{cork_timer = TRef};

maybe_rearm_timer(State=#state{cork_timer = _Ref}) ->
    State.

%% @private
%% @doc Cancels the timer if it was active
maybe_cancel_timer(State=#state{cork_timer = undefined}) ->
    State;

maybe_cancel_timer(State=#state{cork_timer = TRef}) ->
    timer:cancel(TRef),
    State#state{cork_timer = undefined}.

%% @private
%% @doc Recursively extract complete messages from Data, combined with Slice
%%      Slice represents the previous message buffer, if any
%%      Should eagerly extract messages from Data and/or Slice.
-spec decode_data(
    Data :: binary(),
    Slice :: binary()
) -> {ok, [binary()], binary()} | {more, binary()}.

decode_data(Data, Slice) ->
    NewSlice = <<Slice/binary, Data/binary>>,
    case erlang:decode_packet(4, NewSlice, []) of
        {ok, Message, More} ->
            decode_data_inner(More, [Message]);
        _ ->
            {more, NewSlice}
    end.

decode_data_inner(<<>>, Acc) ->
    {ok, Acc, <<>>};

decode_data_inner(Data, Acc) ->
    case erlang:decode_packet(4, Data, []) of
        {ok, Message, More} ->
            decode_data_inner(More, [Message | Acc]);
        _ ->
            {ok, Acc, Data}
    end.

%% @private
%% @doc Reply to the given owners, if any, otherwise drop on the floor
-spec process_messages(Msgs :: [binary()],
                       Owners :: ets:tid(),
                       IdLen :: non_neg_integer(),
                       OwnRef :: reference()) -> ok.
process_messages([], _Owners, _IdLen, _OwnRef) ->
    ok;

process_messages([Msg | Rest], Owners, IdLen, OwnRef) ->
    case Msg of
        <<Id:IdLen, _/binary>> ->
            case ets:take(Owners, Id) of
                [{Id, Callback}] ->
                    Callback(OwnRef, Msg),
                    process_messages(Rest, Owners, IdLen, OwnRef);
                [] ->
                    %% TODO(borja): Deal with unmatched messages?
                    process_messages(Rest, Owners, IdLen, OwnRef)
            end;

        _ ->
            %% TODO(borja): Deal with malformed messages?
            process_messages(Rest, Owners, IdLen, OwnRef)
    end.
