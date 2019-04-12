-module(pipesock_conn).

-behaviour(gen_server).

%% API
-export([open/2,
         open/3,
         close/1,
         get_ref/1,
         send_cb/3,
         send_sync/3]).

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

%% How many bits in each message are reserved as id portion
-define(ID_BITS, 16).

%% Use the equivalent of {packet, 4} to frame the messages
-define(FRAME(Data),
    <<(byte_size(Data)):32/big-unsigned-integer, Data/binary>>).

-record(state, {
    socket :: gen_tcp:socket(),
    cork_timer = undefined :: timer:tref() | undefined,
    %% How long to wait between buffer flush
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

-record(conn_handle, {
    conn_ref :: reference(),
    conn_pid :: pid(),

    %% Mandatory to have this info here to be able to match on the caller side
    %% without validating on the gen_server side.
    id_len :: non_neg_integer()
}).

-opaque conn_handle() :: #conn_handle{}.

-export_type([conn_handle/0]).

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

%% @doc Spawn a new TCP connection
%%
%%      Same as pipesock_conn:open(Ip, Port, #{}).
%%
-spec open(Ip :: atom(), Port :: inet:port_number()) -> {ok, conn_handle()} | {error, Reason :: term()}.
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
%%          The max number of messages sitting in the connection buffer. Once it goes
%%          over this number, the connection will automatically flush it.
%%
%%      cork_len => non_neg_integer()
%%          The max number of milis between buffer flushes.
%%
-spec open(Ip :: atom(),
           Port :: inet:port_number(),
           Options :: conn_opts()) -> {ok, conn_handle()} | {error, Reason :: term()}.

open(Ip, Port, Options) ->
    IdLen = maps:get(id_len, Options, ?ID_BITS),
    Ret = supervisor:start_child(?SUPERVISOR, [Ip, Port, Options]),
    case Ret of
        {ok, Pid} ->
            {ok, Ref} = get_conn_ref(Pid),
            {ok, #conn_handle{conn_ref=Ref, conn_pid=Pid, id_len=IdLen}};

        {error, {already_started, ChildPid}} ->
            {ok, Ref} = get_conn_ref(ChildPid),
            {ok, #conn_handle{conn_ref=Ref, conn_pid=ChildPid, id_len=IdLen}};

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
-spec send_cb(conn_handle(),
              Msg :: binary(),
              Callback :: fun((binary()) -> ok)) -> ok.

send_cb(#conn_handle{conn_pid=Pid, id_len=Len}, Msg, Callback) ->
    <<Id:Len, _/binary>> = Msg,
    gen_server:cast(Pid, {queue, Id, Msg, Callback}).

%% @doc Sync send
%%
%%      Will return when a reply comes or `Timeout` ms pass,
%%      whichever comes first.
%%
-spec send_sync(conn_handle(),
                Msg :: binary(),
                Timeout :: non_neg_integer()) -> {ok, term()}
                                               | {error, timeout}.

send_sync(Conn=#conn_handle{conn_ref=Ref}, Msg, Timeout) ->
    Self = self(),
    send_cb(Conn, Msg, fun(Reply) -> Self ! {ok, Reply} end),
    receive {ok, {Ref, Term}} ->
        {ok, Term}
    after Timeout ->
        {error, timeout}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(term()) -> {ok, state()}| {stop, term()}.
init([IP, Port, Options]) ->
    case gen_tcp:connect(IP, Port, ?SOCK_OPTS) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            Ref = erlang:make_ref(),
            CorkLen = maps:get(cork_len, Options, 5),
            BufferWatermark = maps:get(buf_watermark, Options, 500),
            {ok, #state{self_ref = Ref,
                        socket = Socket,
                        cork_len = CorkLen,
                        msg_id_len = maps:get(id_len, Options, ?ID_BITS),
                        buffer_watermark = BufferWatermark}}
    end.

%% @doc Get back the unique reference of this connection
handle_call(get_ref, _From, State = #state{self_ref=Ref}) ->
    {reply, {ok, Ref}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(E, _From, S) ->
    io:format("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({queue, Id, Msg, Callback}, State = #state{msg_owners=Owners,
                                                       buffer=Buffer,
                                                       buffer_len=Len,
                                                       buffer_watermark=WM}) ->

    NewLen = Len + 1,
    NewBuffer = <<Buffer/binary, (?FRAME(Msg))/binary>>,

    %% Register callback, overriding any old callbacks
    true = ets:insert(Owners, {Id, Callback}),
    NewState = State#state{buffer=NewBuffer, buffer_len=NewLen},
    FinalState = case NewLen =:= WM of
        true ->
            maybe_cancel_timer(flush_buffer(NewState));
        false ->
            maybe_rearm_timer(NewState)
    end,
    {noreply, FinalState};

handle_cast(E, S) ->
    io:format("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(flush_buffer, State) ->
    {noreply, maybe_rearm_timer(maybe_cancel_timer(flush_buffer(State)))};

handle_info({tcp, Socket, Data}, State=#state{socket=Socket,msg_owners=Owners,msg_id_len=IdLen,self_ref=OwnRef}) ->
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
    io:format("unexpected info: ~p~n", [E]),
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
-spec process_messages([binary()], ets:tid(), non_neg_integer(), reference()) -> ok.
process_messages([], _Owners, _IdLen, _OwnRef) ->
    ok;

process_messages([Msg | Rest], Owners, IdLen, OwnRef) ->
    case Msg of
        <<Id:IdLen, _/binary>> ->
            case ets:take(Owners, Id) of
                [{Id, Callback}] ->
                    Callback({OwnRef, Msg}),
                    process_messages(Rest, Owners, IdLen, OwnRef);
                [] ->
                    process_messages(Rest, Owners, IdLen, OwnRef)
            end;

        _ ->
            process_messages(Rest, Owners, IdLen, OwnRef)
    end.