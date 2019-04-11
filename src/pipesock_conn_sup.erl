
-module(pipesock_conn_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([start_link/0,
         init/1]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Worker = #{id => pipesock_conn,
               start => {pipesock_conn, start_link, []},
               restart => transient,
               shutdown => 5000,
               type => worker,
               modules => [pipesock_conn]},
    Strategy = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    {ok, {Strategy, [Worker]}}.
