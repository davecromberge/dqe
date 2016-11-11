-module(dql_deconstruct).

-compile([export_all]).

deconstruct(Query) when is_binary(Query) ->
    case expand_query(Query) of
        {ok, ExpandedQ} ->
            lager:info("Expanded query: ~p~n", [ExpandedQ]),
            R = deconstruct_(ExpandedQ),
            {ok, R};
        {error, {badmatch, _}} ->
            badmatch;
        {error, E} ->
            E
    end.

%%
%% Private functions
%% =================

expand_query(Q) ->
    S = binary_to_list(Q),
    try
        {ok, L, _} = dql_lexer:string(S),
        dql_parser:parse(L)
    catch
        error:Reason ->
            {error, Reason}
    end.

%%selector:
%%
%% -type op :: 'eq' | 'neq' | 'present' | 'AND' | 'OR'
%%
%% -record(condition, {op :: op(),
%%                     args :: [map()]}).
%%
%% -type condition() :: #condition{}.
%%
%% -record(selector, {collection :: binary(),
%%                    metric :: binary(),
%%                    condition :: condition()},
%%
%% -type selector() :: #selector{}.
%%
%% -record(func, {name :: binary,
%%                args :: [any()]}).
%%
%% -type func() :: #func{}.
%%
%%
%%
%% -type query() :: #query{}.
%%
%% parse(L) when is_list(L) ->
%%      [parse(Q) || Q <- L];
%%
%% parse(#{op := dummy}) ->
%%      #{};
%%
%% parse(#{op := get, args := [_B, _M]}, Q) ->
%%     R;
%%
%% parse(#{op := sget, args := [_B, _M]}, Q) ->
%%     R;
%%
%% parse(#{op := lookup, args := [B, undefined]}, Selector) ->
%%     Metric = <<"ALL">>,
%%     Selector#{metric = Metric}.
%% parse(#{op := lookup, args := [B, undefined, Where]}) ->
%%     Metric = <<"ALL">>,
%%     Condition = unparse_where(Where),
%%     Selector#{metric = Metric, condition = Condition}.
%% parse(#{op := lookup, args := [B, M]}) ->
%%     <<(unparse_metric(M))/binary, " FROM '", B/binary, "'">>;
%% parse(#{op := lookup, args := [B, M, Where]}) ->
%%     <<(unparse_metric(M))/binary, " FROM '", B/binary,
%%

-type op() :: 'eq' | 'neq' | 'present' | 'and' | 'or'.

-record(condition, {op :: op(),
                    args :: [term()] }).

-type condition() :: #condition{}.

-record(timeframe, { beginning :: binary(),
                     ending    :: binary(),
                     duration  :: binary() }).

-type timeframe() :: #timeframe{}.

-record(selector, { bucket :: binary(),
                    collection :: binary(),
                    metric :: binary(),
                    condition :: condition() }).

%%-type selector() :: #selector{}.

-record(fn, { name :: binary(),
              args :: [term()] }).

%% -type fn() :: #fn{}.

%% -record(part, { selector :: selector(),
%%                 timeshift :: binary(),
%%                 fn :: fn(),
%%                 alias :: binary() }).

%% -record(query, { parts :: [part()],
%%                  beginning :: binary(),
%%                  ending :: binary(),
%%                  duration :: binary() }).

%% -type query() :: #query{}.

-type lens() :: {Get :: fun(), Put :: fun()}.

-spec access_p(term()) -> lens().
access_p(Key) ->
    {fun(R) -> element(2, lists:keyfind(Key, 1, R)) end,
     fun(A, R) -> lists:keystore(Key, 1, R, {Key, A}) end}.

-spec compose(L1 :: lens(), L2 :: lens()) -> lens().
compose({L1G, L1P}, {L2G, L2P}) ->
    {fun(R) -> L2G(L1G(R)) end,
     fun(A, R) -> L1P(L2P(A, L1G(R)), R) end}.

deconstruct_({ select, Q, [], T}) ->
    #timeframe{beginning = B, ending = E, duration = D} = timeframe(T),
    Parts = deconstruct_(Q),
    #{beginning => B, ending => E, duration => D, parts => Parts};

deconstruct_(L) when is_list(L) ->
    [deconstruct_(Q) || Q <- L];

deconstruct_(#{ op := get, args := [B, M] }) ->
    #selector{bucket = B, metric = M};

deconstruct_(#{ op := sget, args := [B, M] }) ->
    #selector{bucket = B, metric = M};

deconstruct_(#{ op := lookup, args := [B, undefined] }) ->
    #selector{collection = B, metric = <<"ALL">>};

deconstruct_(#{ op := lookup, args := [B, undefined, Where] }) ->
    Condition = deconstruct_where(Where),
    #selector{collection = B, metric = <<"ALL">>, condition = Condition};
deconstruct_(#{op := lookup, args := [B, M] }) ->
    #selector{collection = B, metric = M};
deconstruct_(#{op := lookup, args := [B, M, Where] }) ->
    Condition = deconstruct_where(Where),
    #selector{collection = B, metric = M, condition = Condition};

deconstruct_(#{op := fcall, args := #{name := Name, inputs := Args }}) ->
    Qs = deconstruct_(Args),
    #fn{ name = Name, args = Qs };

deconstruct_(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;
deconstruct_(N) when is_float(N) ->
    <<(float_to_binary(N))/binary>>.

-spec timeframe(map()) -> timeframe().
timeframe(#{op := last, args := [Q] }) ->
    #timeframe{duration = moment(Q)};
timeframe(#{op := between, args := [A, B] }) ->
    #timeframe{beginning = moment(A), ending = moment(B) };
timeframe(#{op := 'after', args := [A, B]}) ->
    #timeframe{beginning = moment(A), duration = moment(B) };
timeframe(#{op := before, args := [A, B]}) ->
    #timeframe{ending = moment(A), duration = moment(B) };
timeframe(#{op := ago, args := [T] }) ->
    #timeframe{beginning = moment(T)}.

moment({time, T, _Unit}) ->
    <<(integer_to_binary(T))/binary>>;
moment(#{op := time, args := [T, _Unit]}) ->
    <<(integer_to_binary(T))/binary>>.

deconstruct_tag({tag, <<>>, K}) ->
    [<<>>, K];
deconstruct_tag({tag, N, K}) ->
    [N, K].

deconstruct_where({'=', T, V}) ->
    #{ op => 'eq', args => [deconstruct_tag(T), V] };
deconstruct_where({'!=', T, V}) ->
    #{ op => 'neq', args => [deconstruct_tag(T), V/binary] };
deconstruct_where({'or', Clause1, Clause2}) ->
    P1 = deconstruct_where(Clause1),
    P2 = deconstruct_where(Clause2),
    #{ op => 'or', args => [ P1, P2 ] };
deconstruct_where({'and', Clause1, Clause2}) ->
    P1 = deconstruct_where(Clause1),
    P2 = deconstruct_where(Clause2),
    #condition{ op = 'and', args = [ P1, P2 ] }.
