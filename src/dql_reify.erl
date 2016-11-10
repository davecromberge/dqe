-module(dql_reify).

-export([reify/1]).

reify(D = #{parts := Parts}) ->
    <<"SELECT ", (reify(Parts))/binary, " ", (reify_timeframe(D))/binary>>;

reify(Parts) when is_list(Parts) ->
    Ps = [reify(P) || P <- Parts],
    combine(Ps);

reify(#{ selector := #{ bucket := B, metric := M, condition := undefined }}) ->
    <<(reify_metric(M))/binary, " BUCKET '", B/binary>>;
reify(#{ selector := #{ bucket := B, metric := M, condition := Cond }}) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " BUCKET '", B/binary, "'", Where/binary>>;
reify(#{ selector := #{ collection := C, metric := M, condition := undefined }}) ->
    <<(reify_metric(M))/binary, " FROM '", C/binary>>;
reify(#{ selector := #{ collection := C, metric := M, condition := Cond }}) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " FROM '", C/binary,
    "'WHERE ", Where/binary>>;

reify(now) ->
    <<"NOW">>;

reify(<<N>>) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;
reify(<<N>>) when is_float(N) ->
    <<(float_to_binary(N))/binary>>.

reify_metric(Ms) when is_list(Ms) ->
    <<".", Result/binary>> = reify_metric(Ms, <<>>),
    Result.
reify_metric(['*' | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".*">>);
reify_metric([Metric | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
reify_metric([], Acc) ->
    Acc.

reify_tag([<<>>, K]) ->
    <<"'", K/binary, "'">>;
reify_tag([N, K]) ->
    <<"'", N/binary, "':'", K/binary, "'">>.
reify_where(#{op := 'eq', args := [T, V] }) ->
    <<(reify_tag(T))/binary, " = '", V/binary, "'">>;
reify_where(#{op := 'neq', args := [T, V] }) ->
    <<(reify_tag(T))/binary, " != '", V/binary, "'">>;
reify_where(#{op := 'or', args := [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " OR (", P2/binary, ")">>;
reify_where(#{op := 'and', args := [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " AND (", P2/binary, ")">>.

reify_timeframe(#{ beginning := undefined, ending := undefined,
                   duration := D }) ->
    <<"LAST ", D/binary>>;
reify_timeframe(#{ beginning := B, ending := undefined, duration := D }) ->
    <<"AFTER ", B/binary, " FOR ", D/binary>>;
reify_timeframe(#{ beginning := undefined, ending := E, duration := D }) ->
    <<"BEFORE ", E/binary, " FOR ", D/binary>>;
reify_timeframe(#{ beginning := B, ending := E }) ->
    <<"BETWEEN ", B/binary, " AND ", E/binary>>.

combine(L) ->
    combine(L, <<>>).

combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).
