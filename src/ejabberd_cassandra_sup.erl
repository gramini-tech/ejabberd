%%%-------------------------------------------------------------------
%%% @author vipinr <vipinr@gramini.com>
%%%
%%%-------------------------------------------------------------------
-module(ejabberd_cassandra_sup).

-behaviour(supervisor).
-behaviour(ejabberd_config).

%% API
-export([start_link/0, 
	 host_up/1, config_reloaded/0, get_pids/0, get_random_pid/0, opt_type/1]).

-define(DEFAULT_POOL_SIZE, 1).
-define(DEFAULT_CASSANDRA_HOST, "127.0.0.1").
-define(DEFAULT_CASSANDRA_PORT, 9042).
-define(DEFAULT_CASSANDRA_KEYSPACE, "step").

%% Supervisor callbacks
-export([init/1]).

-include("logger.hrl").


%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    ?WARNING_MSG("Starting Cassandra Supervisor", []),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

host_up(Host) ->
    case is_cassandra_configured(Host) of
	true ->
	    ejabberd:start_app(cqerl),
	    lists:foreach(
	      fun(Spec) ->
		      supervisor:start_child(?MODULE, Spec)
	      end, get_specs());
	false ->
	    ok
    end.

config_reloaded() ->
    case is_cassandra_configured() of
	true ->
	    ejabberd:start_app(cqerl),
	    lists:foreach(
	      fun(Spec) ->
		      supervisor:start_child(?MODULE, Spec)
	      end, get_specs());
	false ->
	    lists:foreach(
	      fun({Id, _, _, _}) ->
		      supervisor:terminate_child(?MODULE, Id),
		      supervisor:delete_child(?MODULE, Id)
	      end, supervisor:which_children(?MODULE))
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    ejabberd_hooks:add(config_reloaded, ?MODULE, config_reloaded, 20),
    ejabberd_hooks:add(host_up, ?MODULE, host_up, 20),
    Specs = case is_cassandra_configured() of
		true ->
		    ejabberd:start_app(cqerl),
            get_specs();
		false ->
		    []
	    end,
    {ok, {{one_for_one, 1, 1}, Specs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
is_cassandra_configured() ->
    lists:any(fun is_cassandra_configured/1, ejabberd_config:get_myhosts()).

is_cassandra_configured(Host) ->
    ServerConfigured = ejabberd_config:has_option({cassandra_server, Host}),
    PortConfigured = ejabberd_config:has_option({cassandra_port, Host}),
    KeyspaceConfigured = ejabberd_config:has_option({cassandra_keyspace, Host}),
    UserConfigured = ejabberd_config:has_option({cassandra_user, Host}),
    PassConfigured = ejabberd_config:has_option({cassandra_password, Host}),
    ReplicationConfigured = ejabberd_config:has_option({cassandra_replication_factor, Host}),
    PoolConfigured = ejabberd_config:has_option({cassandra_pool_size, Host}),
    ConnTimeoutConfigured = ejabberd_config:has_option(
			      {cassandra_connect_timeout, Host}),
    ServerConfigured or PortConfigured or KeyspaceConfigured or PassConfigured or PoolConfigured or
	UserConfigured or ReplicationConfigured or ConnTimeoutConfigured.

get_specs() ->
    PoolSize = get_pool_size(),
    Server = get_cassandra_server(),
    Port = get_cassandra_port(),
    Keyspace = get_cassandra_keyspace(),
    Username = get_cassandra_username(),
    Password = get_cassandra_password(),
     %%return the child specification
    lists:map(
      fun(I) ->
	     {ejabberd_cassandra:get_proc(I), {ejabberd_cassandra, start_link, 
            [I, Server, Port, Keyspace, Username, Password]},
        transient, 2000, worker, [?MODULE]}
      end, lists:seq(1, PoolSize)).
    
get_pool_size() ->
    ejabberd_config:get_option(cassandra_pool_size, ?DEFAULT_POOL_SIZE).

get_cassandra_server() ->
    ejabberd_config:get_option(cassandra_server, ?DEFAULT_CASSANDRA_HOST).

get_cassandra_keyspace() ->
    ejabberd_config:get_option(cassandra_keyspace, ?DEFAULT_CASSANDRA_KEYSPACE).
get_cassandra_username() ->
    ejabberd_config:get_option(cassandra_username, "").

get_cassandra_password() ->
    ejabberd_config:get_option(cassandra_password, "").

get_cassandra_port() ->
    ejabberd_config:get_option(cassandra_port, ?DEFAULT_CASSANDRA_PORT).

get_pids() ->
    [ejabberd_cassandra:get_proc(I) || I <- lists:seq(1, get_pool_size())].

get_random_pid() ->
    I = randoms:round_robin(get_pool_size()) + 1,
    ejabberd_cassandra:get_proc(I).



iolist_to_list(IOList) ->
    binary_to_list(iolist_to_binary(IOList)).

-spec opt_type(cassandra_password) -> fun((binary()) -> binary());
	      (cassandra_port) -> fun((0..65535) -> 0..65535);
	      (cassandra_user) -> fun((binary()) -> binary());
	      (cassandra_server) -> fun((binary()) -> binary());
	      (cassandra_keyspace) -> fun((binary()) -> binary());
	      (cassandra_replication_factor) -> fun((pos_integer()) -> pos_integer());
	       (atom()) -> [atom()].
opt_type(cassandra_password) -> fun iolist_to_list/1;
opt_type(cassandra_port) ->
    fun (P) when is_integer(P), P > 0, P < 65536 -> P end;
opt_type(cassandra_server) -> fun iolist_to_list/1;
opt_type(cassandra_user) -> fun iolist_to_list/1;
opt_type(cassandra_keyspace) -> fun iolist_to_list/1;
opt_type(cassandra_replication_factor) ->
    fun (I) when is_integer(I), I > 0 -> I end;
opt_type(_) ->
    [cassandra_keyspace, cassandra_password, cassandra_user,
     cassandra_port, cassandra_server].
