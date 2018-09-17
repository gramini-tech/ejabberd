%%%-------------------------------------------------------------------
%%% File    : ejabberd_auth_pam.erl
%%% Author  : Evgeniy Khramtsov <xram@jabber.ru>
%%% Purpose : PAM authentication
%%% Created : 5 Jul 2007 by Evgeniy Khramtsov <xram@jabber.ru>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2018   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%-------------------------------------------------------------------
-module(ejabberd_auth_meerkat).

-behaviour(ejabberd_config).

-author('vipinr@gramini.com').

-behaviour(ejabberd_auth).

-include("logger.hrl").
-include("meerkat.hrl").

-export([start/1, stop/1, check_password/4, use_cache/1,
	 user_exists/2, store_type/1, plain_password_required/1,
	 opt_type/1]).

-export([prepared_queries/0]).

start(_Host) ->
    ok.

stop(_Host) ->
    ok.

use_cache(Host) ->
    false.

check_password(User, AuthzId, Host, Password) ->
    ?INFO_MSG("Got User ~p, AuthzId ~p, Host ~p, Password ~p",[User, AuthzId, Host, Password]),
    if AuthzId /= <<>> ->
        {ok, Row} = ejabberd_cassandra:cql_read_one(?MODULE, get_user, [{user_jid, ?JID_UDT(User, Host)}]),
        if length(Row) > 0 -> 
            true;
        true ->
            false 
        end;
    true ->
        false
    end.

user_exists(User, Host) -> 
    {ok, Row} = ejabberd_cassandra:cql_read_one(?MODULE, get_user, [{user_jid, ?JID_UDT(User, Host)}]),
    if length(Row) > 0 -> 
        true;
    true ->
        false 
    end.

plain_password_required(_) -> true.

store_type(_) -> external.

prepared_queries() ->
    [{get_users, "select * from user;"},
     {get_user, "select * from user where user_jid= ?;"}
    ].

%%====================================================================
%% Internal functions
%%====================================================================
-spec opt_type(pam_service) -> fun((binary()) -> binary());
	      (pam_userinfotype) -> fun((username | jid) -> username | jid);
	      (atom()) -> [atom()].
opt_type(pam_service) -> fun iolist_to_binary/1;
opt_type(pam_userinfotype) ->
    fun (username) -> username;
	(jid) -> jid
    end;
opt_type(_) -> [pam_service, pam_userinfotype].
