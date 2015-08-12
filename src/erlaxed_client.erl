%%
%%  @author    Jan Krueger <jan.krueger@gmail.com>
%%  @copyright 2015 Jan Krueger
%%
%%% This file is part of erlaxed released under the MIT license.
%%

-module(erlaxed_client).
-compile({no_auto_import,[put/1, put/2]}).
-export([delete/1, delete/2, delete/3]).
-export([get/1, get/2, get/3]).
-export([put/1, put/2, put/3]).
-export([post/2]).

-define(DEFAULT_TIMEOUT, 5000).

delete(Connection) ->
    delete(Connection, <<>>).

delete(Connection, Endpoint) ->
    URL      = endpoint(Connection, Endpoint),
    Response = httpc:request(delete, {URL, []}, [], []),
    interpret_response(Response).

delete(Connection, Endpoint0, Rev) ->
    Endpoint = endpoint(Connection, Endpoint0),
    URL      = Endpoint ++ "?rev=" ++ etbx:to_string(Rev),
    Response = httpc:request(delete, {URL, []}, [], []),
    interpret_response(Response).

get(Connection) ->
    get(Connection, <<>>).

get(Connection, Endpoint) ->
    get(Connection, Endpoint, []).

get(Connection, Endpoint, Options) ->
    URL      = query(endpoint(Connection, Endpoint), Options),
    Response = httpc:request(get, {URL, []}, [], []),
    interpret_response(Response).

put(Connection) ->
    put(Connection, <<>>).

put(Connection, Endpoint) ->
    send(Connection, Endpoint, put, <<>>).

put(Connection, Endpoint, Doc) ->
    send(Connection, Endpoint, put, jiffy:encode(Doc)).

post(Connection, Doc) ->
    post(Connection, <<>>, Doc).

post(Connection, Endpoint, Doc) ->
    send(Connection, Endpoint, post, jiffy:encode(Doc)).

%% @private
send(Connection, Endpoint, Method, Body) ->
    URL      = endpoint(Connection, Endpoint),
    Response = httpc:request(
                 Method, {URL, [], "application/json", Body},
                 [], []),
    interpret_response(Response).

%% @private
endpoint(Connection, Endpoint) ->
    Server = etbx:get_value(host, Connection),
    DB     = etbx:get_value(db,   Connection),
    etbx:to_string(endpoint(Server, DB, Endpoint)).
endpoint(Server, undefined, <<>>) ->
    Server;
endpoint(Server, undefined, Endpoint) ->
    <<Server/binary, "/", Endpoint/binary>>;
endpoint(Server, DB, <<>>) ->
    <<Server/binary, "/", DB/binary>>;
endpoint(Server, DB, Endpoint) ->
    <<Server/binary, "/", DB/binary, "/", Endpoint/binary>>.

%% @private
query(URL, []) ->
    URL;
query(URL0, Options) ->
    URL   = string:concat(URL0, "?"),
    Query = lists:map(fun encode_option/1, Options),
    string:concat(URL, string:join(Query, "&")).

%% @private
encode_option({key, Value}) ->
    format_option("key", jiffy:encode(Value));
encode_option({startkey, Value}) ->
    format_option("startkey", jiffy:encode(Value));
encode_option({endkey, Value}) ->
    format_option("endkey", jiffy:encode(Value));
encode_option({keys, Value}) when is_list(Value) ->
    format_option("keys", jiffy:encode(Value));
encode_option({Option, Value}) ->
    format_option(Option, Value);
encode_option(Option) ->
    etbx:to_string(Option).

format_option(Option, Value) ->
    Query = etbx:to_string(Option),
    Param = edoc_lib:escape_uri(etbx:to_string(Value)),
    Query ++ "=" ++ Param.

%% @private
interpret_response({ok, {{_Version, Status, _Line}, _Headers, Body}}) ->
    interpret_response({ok, {Status, Body}});
interpret_response({ok, {200, Body}}) ->
    {ok, jiffy:decode(Body)};
interpret_response({ok, {201, Body}}) ->
    {ok, jiffy:decode(Body)};
interpret_response({ok, {204, _}}) ->
    ok;
interpret_response({ok, {400, _}}) ->
    {error, bad_request};
interpret_response({ok, {404, _}}) ->
    {error, not_found};
interpret_response({ok, {409, _}}) ->
    {error, conflict};
interpret_response({ok, {500, _}}) ->
    {error, internal_error};
interpret_response({ok, {502, _}}) ->
    {error, bad_gateway};
interpret_response({ok, {503, _}}) ->
    {error, busy};
interpret_response({error, Reason}) ->
    {error, Reason}.
