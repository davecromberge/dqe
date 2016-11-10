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
%%  {collection
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
%%       "' WHERE ", (unparse_where(Where))/binary>>.

-record(timeframe, { beginning :: binary(),
                     ending    :: binary(),
                     duration  :: binary() }).

%% -type op() :: 'eq' | 'neq' | 'present' | 'AND' | 'OR'
%% -record(condition, { op :: op(),
%%                      args :: [term()] }.
%%
%% -type timeframe() :: #timeframe{}.

%% -record(selector, {collection :: binary(),
%%                    metric :: binary(),
%%                    condition :: binary()}).
%%
%% -type selector() :: #selector{}.
%%
%% -record(part, { selector :: selector(),
%%                 timeshift :: binary(),
%%                 func :: binary(),
%%                 alias :: binary()}).
%%
%% -type part() :: #part{}.

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


deconstruct_(Q) ->
    R = #{ parts => [] },
    deconstruct_(Q, R).

deconstruct_(L, R) when is_list(L) ->
    lists:foldl(
      fun(Q, Parts) ->
        Part = #{ selector => undefined},
        deconstruct_(Q, R#{parts := [Part | Parts]})
      end, [], L);

deconstruct_({ select, Q, [], T}, R) ->
    #timeframe{beginning = B, ending = E, duration = D} = timeframe(T),
    deconstruct_(Q, R#{beginning => B, ending => E, duration => D});

deconstruct_(#{ op := get, args := [B, M] },
             R = #{parts := [Part | Rest]}) ->
    Selector = #{bucket => B, metric => M},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]};

deconstruct_(#{ op := sget, args := [B, M] },
             R = #{parts := [Part | Rest]}) ->
    Selector = #{bucket => B, metric => M},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]};

deconstruct_(#{ op := lookup, args := [B, undefined] },
             R = #{parts := [Part | Rest]}) ->
    Selector = #{collection => B, metric => <<"ALL">>},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]};
deconstruct_(#{ op := lookup, args := [B, undefined, Where] },
             R = #{parts := [Part | Rest]}) ->
    Condition = deconstruct_where(Where),
    Selector = #{collection => B, metric => <<"ALL">>, condition => Condition},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]};
deconstruct_(#{op := lookup, args := [B, M]},
             R = #{parts := [Part | Rest]}) ->
    Selector = #{collection => B, metric => M},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]};
deconstruct_(#{op := lookup, args := [B, M, Where]},
             R = #{parts := [Part | Rest]}) ->
    Condition = deconstruct_where(Where),
    Selector = #{collection => B, metric => M, condition => Condition},
    Part0 = Part#{selector := Selector},
    R#{parts := [Part0 | Rest]}.

timeframe(#{op := last, args := [Q]}) ->
    #timeframe{duration = moment(Q)};
timeframe(#{op := between, args := [A, B]}) ->
    #timeframe{beginning = moment(A), ending = moment(B)};
timeframe(#{op := 'after', args := [A, B]}) ->
    #timeframe{beginning = moment(A), duration = moment(B)};
timeframe(#{op := before, args := [A, B]}) ->
    #timeframe{ending = moment(A), duration = moment(B)};
timeframe(#{op := ago, args := [T]}) ->
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
    #{ op => 'and', args => [ P1, P2 ] }.
