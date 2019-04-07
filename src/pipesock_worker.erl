-module(pipesock_worker).

-behaviour(gen_server).

%% API
-export([start_link/2,
         send_cb/3,
         send_sync/3,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("pipesock.hrl").

%% Socket options
%% active and deliver_term set options for receiving data
%% packet => raw, since we're doing the framing ourselves
%% (see decode_data/2)
-define(SOCK_OPTS, [binary,
                    {active, once},
                    {deliver, term},
                    {packet, raw}]).

%% Use the equivalent of {packet, 4} to frame the messages
-define(FRAME(Data),
    <<(byte_size(Data)):32/big-unsigned-integer, Data/binary>>).

-record(state, {
    socket :: gen_tcp:socket(),
    cork_timer = undefined :: timer:tref() | undefined,
    cork_len = ?CORK_LEN :: non_neg_integer(),
    msg_owners = ets:new(msg_owners, [set, private]) :: ets:tid(),

    %% Buffer and ancillary state
    buffer = <<>> :: binary(),
    buffer_watermark = ?BUF_WATERMARK :: non_neg_integer(),
    buffer_len = 0 :: non_neg_integer(),

    %% Incomplete data coming from socket (since we're framing)
    message_slice = <<>> :: binary()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(IP :: inet:ip_address(),
                 Port :: inet:port_number()) -> {ok, pid()} | {error, term()}.
start_link(IP, Port) ->
    gen_server:start_link(?MODULE, [IP, Port], []).

%% @doc Async send
%%
%%      Accepts an optional callback that is fired when a reply
%%      to this message is delivered.
%%
-spec send_cb(Ref :: pid(),
              Msg :: binary(),
              Callback :: fun((binary()) -> ok)) -> ok.

send_cb(Ref, <<Id:?ID_BITS, _/binary>>=Msg, Callback) ->
    gen_server:cast(Ref, {queue, Id, Msg, Callback}).

%% @doc Sync send
%%
%%      Will return when a reply comes or `Timeout` ms pass,
%%      whichever comes first.
%%
-spec send_sync(Ref :: pid(),
                Msg :: binary(),
                Timeout :: non_neg_integer()) -> {ok, term()}
                                               | {error, timeout}.

send_sync(Ref, Msg, Timeout) ->
    Self = self(),
    send_cb(Ref, Msg, fun(Reply) -> Self ! {ok, Reply} end),
    receive
        {ok, Term} ->
            {ok, Term}
    after Timeout ->
        {error, timeout}
    end.

%% @doc Close the TCP connection
-spec close(Ref :: pid()) -> ok.
close(Ref) ->
    gen_server:cast(Ref, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(term()) -> {ok, state()}| {stop, term()}.
init([IP, Port]) ->
    case gen_tcp:connect(IP, Port, ?SOCK_OPTS) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            {ok, #state{socket = Socket}}
    end.

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

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    io:format("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(flush_buffer, State) ->
    {noreply, maybe_rearm_timer(maybe_cancel_timer(flush_buffer(State)))};

handle_info({tcp, Socket, Data}, State=#state{socket=Socket}) ->
    NewSlice = case decode_data(Data, State#state.message_slice) of
        {more, Rest} ->
            Rest;
        {ok, Msgs, Rest} ->
            ok = process_messages(Msgs, State#state.msg_owners),
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

%% Flushes the data buffer through the socket and reset the state
-spec flush_buffer(state()) -> state().
flush_buffer(State = #state{socket=Socket, buffer=Buffer}) ->
    ok = gen_tcp:send(Socket, Buffer),
    State#state{buffer = <<>>, buffer_len = 0}.

%% Rearms the timer if it was not yet active
-spec maybe_rearm_timer(state()) -> state().
maybe_rearm_timer(State=#state{cork_timer = undefined, cork_len = Span}) ->
    {ok, TRef} = timer:send_after(Span, flush_buffer),
    State#state{cork_timer = TRef};

maybe_rearm_timer(State=#state{cork_timer = _Ref}) ->
    State.

%% Cancels the timer if it was active
maybe_cancel_timer(State=#state{cork_timer = undefined}) ->
    State;

maybe_cancel_timer(State=#state{cork_timer = TRef}) ->
    timer:cancel(TRef),
    State#state{cork_timer = undefined}.

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

%% @doc Reply to the given owners, if any, otherwise drop on the floor
-spec process_messages([binary()], ets:tid()) -> ok.
process_messages([], _Owners) ->
    ok;

process_messages([Msg | Rest], Owners) ->
    case Msg of
        <<Id:?ID_BITS, _/binary>> ->
            case ets:take(Owners, Id) of
                [{Id, Callback}] ->
                    Callback(Msg),
                    process_messages(Rest, Owners);
                [] ->
                    process_messages(Rest, Owners)
            end;

        _ ->
            process_messages(Rest, Owners)
    end.
