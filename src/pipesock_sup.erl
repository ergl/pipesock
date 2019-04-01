%%%-------------------------------------------------------------------
%% @doc pipesock top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pipesock_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SockSup = #{id => pipesock_worker_sup,
                start => {pipesock_worker_sup, start_link, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [pipesock_worker_sup]},
    Strategy = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {Strategy, [SockSup]}}.

%%====================================================================
%% Internal functions
%%====================================================================
