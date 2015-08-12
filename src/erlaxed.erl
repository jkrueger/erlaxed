%%
%%  @author    Jan Krueger <jan.krueger@gmail.com>
%%  @copyright 2015 Jan Krueger
%%
%%% This file is part of erlaxed released under the MIT license.
%%

-module(erlaxed).
-export([config/0, config/1, config/2]).
-export([connect/2]).
-export([create/2, create/3]).
-export([delete/1, delete/2]).
-export([fetch/2]).
-export([store/2, store/3]).
-export([view/2, view/3]).

-type proplist()::list(tuple()).

-spec config() -> any().
config() ->
    config(<<"http://127.0.0.1:5984">>, []).

-spec config(binary()) -> any().
config(Host) ->
    config(Host, []).

-spec config(string() | binary(), map()) -> any().
config(Host, Options) ->
    #{ host    => etbx:to_binary(Host),
       options => Options }.

-spec connect(any(), binary() | string()) -> any().
connect(Config, Name0) ->
    Name = etbx:to_binary(Name0),
    {ok, etbx:update(db, Name, Config)}.

create(Config, Name) ->
    create(Config, Name, []).

create(Config, Name, Options) ->
    ensure_db(connect(Config, Name), Options).

delete(DB) ->
    strip_body(erlaxed_client:delete(DB)).

delete(DB, Doc) ->
    Id  = get_value(<<"_id">>, Doc),
    Rev = get_value(<<"_rev">>, Doc),
    if ((Id =:= undefined) or (Rev =:= undefined)) ->
            {error, not_a_doc};
       true ->
            erlaxed_client:delete(DB, Id, Rev)
    end.

fetch(DB, Id) ->
    erlaxed_client:get(DB, Id).

store(DB, Doc) ->
    store(DB, get_value(<<"_id">>, Doc), Doc).

store(DB, undefined, Doc) ->
    return_doc(DB, erlaxed_client:post(DB, to_json(Doc)));
store(DB, Id, Doc) ->
    erlaxed_client:put(DB, Id, to_json(Doc)).

%% @private
ensure_db(DB, Options) ->
    ensure_db(DB, strip_body(erlaxed_client:get(DB)), Options).

ensure_db(DB, ok, _) ->
    {ok, DB};
ensure_db(_DB, {error, not_found}, [exclusive]) ->
    {error, database_exists};
ensure_db(DB, {error, not_found}, _Options) ->
    maybe(erlaxed_client:put(DB), DB);
ensure_db(Error, _, _) ->
    Error.

view(DB, View) ->
    view(DB, View, []).

view(DB, {Design0, View0}, Options) ->
    Design = etbx:to_binary(Design0),
    View   = etbx:to_binary(View0),
    Endpoint = <<"_design/", Design/binary, "/_view/", View/binary>>,
    erlaxed_client:get(DB, Endpoint, Options).

%% @private
return_doc(DB, {ok, Handle}) ->
    return_doc(DB, get_value(<<"id">>, Handle));
return_doc(_, undefined) ->
    throw({error, api, "POST on a document did not return an id"});
return_doc(DB, Id) when is_binary(Id) ->
    erlaxed_client:get(DB, Id);
return_doc(_, Error) ->
    Error.

%% @private
maybe(ok, X) ->
    {ok, X};
maybe({ok, _}, X) ->
    {ok, X};
maybe(Else, _) ->
    Else.

%% @private
strip_body(ok) ->
    ok;
strip_body({ok, _}) ->
    ok;
strip_body(Else) ->
    Else.

%% @private
-spec to_json(map() | proplist() | tuple()) -> tuple().
to_json(M) when is_map(M) ->
    {maps:to_list(M)};
to_json(L) when is_list(L) ->
    {L};
to_json({L} = X) when is_list(L) ->
    X.

%% @private
get_value(K, {Doc}) ->
    etbx:get_value(K, Doc);
get_value(K, Doc) ->
    etbx:get_value(K, Doc).
