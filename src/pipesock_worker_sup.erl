
-module(pipesock_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_connection/2]).

%% Supervisor callbacks
-export([init/1]).


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
    Worker = #{id => pipesock_worker,
               start => {pipesock_worker, start_link, []},
               restart => transient,
               shutdown => 5000,
               type => worker,
               modules => [pipesock_worker]},
    Strategy = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    {ok, {Strategy, [Worker]}}.

-spec start_connection(IP :: inet:ip_address(),
                       Port :: inet:port_number()) -> {ok, pid()}.

start_connection(IP, Port) ->
    supervisor:start_child(?MODULE, [IP, Port]).
