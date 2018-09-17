%%-------------------------------------------------------------------------------------------------
%% Macros and records for the meerkat system
%%-------------------------------------------------------------------------------------------------

%% create JID User Defined Type to be used with cassandra
-define(JID_UDT(User, Host),
        #{uid => User, domain => Host}).
%%        io:format("{uid: '~s', domain: '~s'}", [User, Host])).
