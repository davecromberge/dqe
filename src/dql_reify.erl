-module(dql_reify).

-export([reify/1]).

-include("dl_query_model.hrl").

reify(D = #query{ parts = Parts }) ->
    Reified = <<"SELECT ", (reify(Parts))/binary, " ",
                (reify_timeframe(D))/binary>>,
    {ok, Reified};

reify(Parts) when is_list(Parts) ->
    Ps = [reify(P) || P <- Parts],
    combine(Ps);

reify(#part{ selector = #selector{ collection = undefined, bucket = B,
                                   metric = M, condition = undefined }}) ->
    <<(reify_metric(M))/binary, " BUCKET '", B/binary, "'">>;

reify(#part{ selector = #selector{ collection = undefined, bucket = B,
                                   metric = M, condition = Cond }}) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " BUCKET '", B/binary, "'", Where/binary>>;

reify(#part{ selector = #selector{ collection = C, bucket = undefined,
                                   metric = M, condition = undefined }}) ->
    <<(reify_metric(M))/binary, " FROM '", C/binary, "'">>;

reify(#part{ selector = #selector{ collection = C, bucket = undefined,
                                   metric = M, condition = Cond }}) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " FROM '", C/binary, "' WHERE ", Where/binary>>;

reify(#part{ fn = #fn { name = Name, args = Args }}) ->
    Qs = reify(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

reify(N) when is_integer(N) ->
    <<(integer_to_binary(N))/binary>>;
reify(N) when is_float(N) ->
    <<(float_to_binary(N))/binary>>;
reify(now) ->
    <<"NOW">>;
reify(N) ->
    N.

-spec reify_metric(['*' | binary()]) -> binary().
reify_metric(Ms) when is_list(Ms) ->
    <<".", Result/binary>> = reify_metric(Ms, <<>>),
    Result.
-spec reify_metric(['*' | binary()], binary()) -> binary().
reify_metric(['*' | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".*">>);
reify_metric([Metric | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
reify_metric([], Acc) ->
    Acc.

-spec reify_tag([binary(),...]) -> <<_:16,_:_*8>>.
reify_tag([<<>>, K]) ->
    <<"'", K/binary, "'">>;
reify_tag([N, K]) ->
    <<"'", N/binary, "':'", K/binary, "'">>.

%% -spec reify_where(condition()) -> binary().
reify_where(#condition{ op = 'eq', args = [T, V] }) ->
    <<(reify_tag(T))/binary, " = '", V/binary, "'">>;
reify_where(#condition{ op = 'neq', args = [T, V] }) ->
    <<(reify_tag(T))/binary, " != '", V/binary, "'">>;
reify_where(#condition{ op = 'or', args = [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " OR (", P2/binary, ")">>;
reify_where(#condition{ op = 'and', args = [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " AND (", P2/binary, ")">>.

-spec reify_timeframe(query()) -> binary().
reify_timeframe(#query{ beginning = undefined, ending = undefined,
                        duration = D }) ->
    <<"LAST ", (reify(D))/binary>>;
reify_timeframe(#query{ beginning = B, ending = undefined, duration = D }) ->
    <<"AFTER ", (reify(B))/binary, " FOR ", (reify(D))/binary>>;
reify_timeframe(#query{ beginning = undefined, ending = E, duration = D }) ->
    <<"BEFORE ", (reify(E))/binary, " FOR ", (reify(D))/binary>>;
reify_timeframe(#query{ beginning = B, ending = E }) ->
    <<"BETWEEN ", (reify(B))/binary, " AND ", (reify(E))/binary>>.

-spec combine(list(binary())) -> binary().
combine(L) ->
    combine(L, <<>>).

-spec combine(list(binary()), binary()) -> binary().
combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).
