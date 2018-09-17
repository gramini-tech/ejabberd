%%%-------------------------------------------------------------------
%%% File    : ejabberd_cassandra.erl
%%% Author  : vipinr(vipinnr@gramini.com)
%%%
%%%----------------------------------------------------------------------

-module(ejabberd_cassandra).
-ifndef(GEN_SERVER).
-define(GEN_SERVER, gen_server).
-endif.
-behaviour(?GEN_SERVER).

-include_lib("cqerl/include/cqerl.hrl").

%% ====================================================================
%% Types
%% ====================================================================

-type row()                 :: #{atom() => term()}.
-type rows()                :: [row()].
-type parameters()          :: proplists:proplist() | row().
-type fold_fun()            :: fun((Page :: rows(), fold_accumulator()) -> fold_accumulator()).
-type fold_accumulator()    :: any().
-type client()              :: cqerl:client().
-type query_name()          :: atom() | {atom(), atom()}.
-type cql_query()           :: #cql_query{}.
-type cql_result()          :: [proplists:proplist()].
-type cql_schema_changed()  :: #cql_schema_changed{}.


%% ====================================================================
%% Definitions
%% ====================================================================

-define(WRITE_RETRY_COUNT, 3).
-define(READ_RETRY_COUNT, 3).
-define(SERVER, ?MODULE).
-define(PROCNAME, 'ejabberd_cassandra').
-define(CALL_TIMEOUT, 60*1000). %% 60 seconds

-record(state, {client :: client(),
                query_str :: binary() | undefined,
                params :: parameters() | undefined, 
                opts = #{} :: map()
            }).

-type state() ::#state{}.

%% ====================================================================
%% Exports
%% ====================================================================

%% API
-export([start_link/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, get_proc/1]).


%% API
-export([cql_read/3, cql_read_one/3, cql_write/3, cql_write_async/3]).
-export([now_timestamp/0]).

%% Types
-export_type([ query_name/0 ]).
-export_type([row/0, rows/0, parameters/0, fold_fun/0, fold_accumulator/0]).

%% Callbacks definitions
-callback prepared_queries() -> list({term(), string()}).

-include("logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================
start_link(Num, Server, Port, Keyspace, Username, Password) ->
    ?GEN_SERVER:start_link({local, get_proc(Num)}, ?MODULE, [Server, Port, Keyspace, Username, Password], []).

get_proc(I) ->
    misc:binary_to_atom(
      iolist_to_binary(
	[atom_to_list(?MODULE), $_, integer_to_list(I)])).



%%%===================================================================
%%% Cassandra commands API
%%%===================================================================

%% --------------------------------------------------------
%% @doc Execute batch write query to cassandra (insert, update or delete).
%% --------------------------------------------------------

-spec cql_write(Module :: atom(), QueryName :: query_name(), Rows :: [parameters()]) ->
                       ok | {error, Reason :: any()}.
cql_write(Module, QueryName, Rows)  ->
    QueryStr = proplists:get_value(QueryName, Module:prepared_queries()),
    Opts = #{
      retry   => ?WRITE_RETRY_COUNT
     },
    write(get_random_client(), QueryStr, Rows).
%% --------------------------------------------------------
%% @doc Execute async batch write query to cassandra (insert, update or delete).
%% --------------------------------------------------------
-spec cql_write_async(Module :: atom(),
                      QueryName :: query_name(), Rows :: [parameters()]) ->
                             ok | {error, Reason :: any()}.
cql_write_async(Module, QueryName, Rows)  ->
    QueryStr = proplists:get_value(QueryName, Module:prepared_queries()),
    Opts = #{
      retry   => ?WRITE_RETRY_COUNT
     },
    gen_server:cast(self(), {write_sync, QueryStr, Rows}).

%% --------------------------------------------------------
%% @doc Execute read query to cassandra (select).
%% Returns all rows at once even if there are several query pages.
%% --------------------------------------------------------
-spec cql_read(Module :: atom(),
               QueryName :: query_name(), Params :: parameters()) ->
                      {ok, Rows :: rows()} | {error, Reason :: any()}.
cql_read (Module, QueryName, Params)  ->
    QueryStr = proplists:get_value(QueryName, Module:prepared_queries()),
    read(get_random_client(), QueryStr, Params).

-spec cql_read_one(Module :: atom(),
               QueryName :: query_name(), Params :: parameters()) ->
                      {ok, Rows :: rows()} | {error, Reason :: any()}.
cql_read_one (Module, QueryName, Params)  ->
    QueryStr = proplists:get_value(QueryName, Module:prepared_queries()),
    read_one(get_random_client(), QueryStr, Params).

%% @doc Return timestamp in microseconds
now_timestamp() ->
    now_to_usec(os:timestamp()).

-spec now_to_usec(erlang:timestamp()) -> non_neg_integer().
now_to_usec({MSec, Sec, USec}) ->
    (MSec*1000000 + Sec)*1000000 + USec.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Server, Port, Keyspace, Username, Password]) ->
    process_flag(trap_exit, true), 
    self()! {connect, Server, Port, Keyspace, Username, Password},
    {ok, #state{}}.

handle_call(_,_From, #state{client = undefined} = State) ->
    ?ERROR_MSG("Not connected to cassandra DB", []),
    {stop, State};
%% The only useful call, to get the client associated with the Pid
handle_call(get_client, _From, #state{client = Client} = State) ->
    {reply, {ok, Client}, State};
handle_call(Request,_From, State) ->
    ?WARNING_MSG("Unknown call ~p", [Request]),
    {noreply, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.



handle_info({connect, Server, Port, Keyspace, Username, Password}, #state{client = undefined} = State) ->
    case connect(State, Server, Port, Keyspace, Username, Password) of 
    {ok, Client} ->
        {noreply, State#state{client = Client}};
    {error, Reason} -> 
        ?ERROR_MSG("Error happened: ~p", [Reason]),
        {stop, error, State}
    end;
handle_info(Info, State) ->
    ?WARNING_MSG("unexpected info = ~p", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec connect(state(), binary(), non_neg_integer(), binary(), binary(), binary()) -> {ok, client()} | {error, any()}.
connect(#state{client=undefined}, Server, Port, Keyspace, User, Password) ->

    ?INFO_MSG("Trying to connect to Cassandra@[~p, ~p], Keyspace: ~p, User: ~p]", [Server, Port, Keyspace, User]),
   
    try case cqerl:get_client({Server, Port}, [{keyspace, list_to_atom(Keyspace)}]) of
	    {ok, Client} ->
		?INFO_MSG("Connection established to Cassandra at ~s:~p",
		       [Server, Port]),
		{ok, Client};
	    {error, Why} ->
        ?ERROR_MSG("Error connecting ~p", [Why]),
		erlang:error(Why)
	end
    catch _:Reason ->
        ?ERROR_MSG("Exception connecting ~p", [Reason]),
	    {error, Reason}
    end;
connect(#state{client=Client}, _,_,_,_,_) -> 
    {ok, Client}.

-spec read(client(), binary(), parameters()) -> {ok, [proplists:proplist()]} | {error, any()}.
read (Client, QueryStr, Params) ->
    case execute_query(Client, #cql_query{statement=QueryStr, values=Params}) of
        {ok, Result} -> 
            {ok, cqerl:all_rows(Result)};
        {_, Reason} ->
            ?WARNING_MSG("Something went wrong. ~p", [Reason]),
            {error, Reason}
    end.

read_one (Client, QueryStr, Params) ->
    case execute_query(Client, #cql_query{statement=QueryStr, values=Params}) of
        {ok, Result} -> 
            {ok, cqerl:head(Result)};
        {_, Reason} ->
            ?WARNING_MSG("Something went wrong. ~p", [Reason]),
            {error, Reason}
    end.

%% internal function
execute_query(Client, Query) ->
     cqerl:run_query(Client, Query).

-spec write(client(), binary(), parameters()) -> {ok, cql_result()} | {error, any()}.
write (Client, QueryStr, Row) when is_list(Row), length(Row) == 1 ->
    case cqerl:run_query(Client, #cql_query{statement=QueryStr, values=Row}) of
        {ok, Result} -> 
            ?INFO_MSG("Write result ~p", [Result]),
            {ok, Result};
        {_, Reason} ->
            ?WARNING_MSG("Something went wrong. ~p", [Reason])
    end.

write_async(#state{})  -> 
    ok.

get_random_client() ->
    PoolPid = ejabberd_cassandra_sup:get_random_pid(),
    get_cassandra_client(PoolPid).

get_cassandra_client(PoolPid) ->
    case catch gen_server:call(PoolPid, get_client) of
	{ok, Client} ->
	    Client;
	{'EXIT', {timeout, _}} ->
	    throw({error, timeout});
	{'EXIT', Err} ->
	    throw({error, Err})
    end.
