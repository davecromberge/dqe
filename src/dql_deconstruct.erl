-module(dql_deconstruct).

-include("dl_query_model.hrl").

-compile([export_all]).

-spec deconstruct(binary()) -> {ok, query()} | badmatch | term().
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

-spec expand_query(binary()) -> {ok, map()} | {error, term()}.
expand_query(Q) ->
    S = binary_to_list(Q),
    try
        {ok, L, _} = dql_lexer:string(S),
        dql_parser:parse(L)
    catch
        error:Reason ->
            {error, Reason}
    end.

access_p(Key) ->
    {fun(R) -> element(2, lists:keyfind(Key, 1, R)) end,
     fun(A, R) -> lists:keystore(Key, 1, R, {Key, A}) end}.

-spec compose(L1 :: lens(), L2 :: lens()) -> lens().
compose({L1G, L1P}, {L2G, L2P}) ->
    {fun(R) -> L2G(L1G(R)) end,
     fun(A, R) -> L1P(L2P(A, L1G(R)), R) end}.

deconstruct_({ select, Q, [], T}) ->
    #timeframe{ beginning = B, ending = E, duration = D } = timeframe(T),
    Parts = deconstruct_(Q),
    #query{ beginning = B, ending = E, duration = D, parts = Parts };

deconstruct_(L) when is_list(L) ->
    [begin
         Deconstructed = deconstruct_(Q),
         case Deconstructed of
            #selector{} ->
                #part{ selector = Deconstructed };
            #fn{} ->
                #part{ fn = Deconstructed };
            _ ->
                Deconstructed
        end
     end || Q <- L];

deconstruct_(#{ op := get, args := [B, M] }) ->
    #selector{ bucket = B, metric = M };

deconstruct_(#{ op := sget, args := [B, M] }) ->
    #selector{ bucket = B, metric = M };

deconstruct_(#{ op := lookup, args := [B, undefined] }) ->
    #selector{ collection = B, metric = [<<"ALL">>] };

deconstruct_(#{ op := lookup, args := [B, undefined, Where] }) ->
    Condition = deconstruct_where(Where),
    #selector{ collection = B, metric = [<<"ALL">>], condition = Condition };
deconstruct_(#{ op := lookup, args := [B, M] }) ->
    #selector{ collection = B, metric = M };
deconstruct_(#{ op := lookup, args := [B, M, Where] }) ->
    Condition = deconstruct_where(Where),
    #selector{ collection = B, metric = M, condition = Condition };

deconstruct_(#{op := fcall, args := #{name := Name, inputs := Args }}) ->
    Qs = deconstruct_(Args),
    #fn{ name = Name, args = Qs };

deconstruct_(N) when is_integer(N)->
    N;
deconstruct_(N) when is_float(N) ->
    N;
deconstruct_({time, T, U}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
deconstruct_(#{op := time, args := [T, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
deconstruct_(now) ->
    now.

-spec timeframe(map()) -> timeframe().
timeframe(#{ op := last, args := [Q] }) ->
    #timeframe{ duration = deconstruct_(Q) };
timeframe(#{ op := between, args := [A, B] }) ->
    #timeframe{ beginning = deconstruct_(A), ending = deconstruct_(B) };
timeframe(#{ op := 'after', args := [A, B] }) ->
    #timeframe{ beginning = deconstruct_(A), duration = deconstruct_(B) };
timeframe(#{ op := before, args := [A, B] }) ->
    #timeframe{ ending = deconstruct_(A), duration = deconstruct_(B) };
timeframe(#{ op := ago, args := [T] }) ->
    #timeframe{ beginning = deconstruct_(T) }.

-spec deconstruct_tag({tag, binary(), binary()}) -> list(binary()).
deconstruct_tag({tag, <<>>, K}) ->
    [<<>>, K];
deconstruct_tag({tag, N, K}) ->
    [N, K].

deconstruct_where({'=', T, V}) ->
    #condition{ op = 'eq', args = [deconstruct_tag(T), V] };
deconstruct_where({'!=', T, V}) ->
    #condition{ op = 'neq', args = [deconstruct_tag(T), V] };
deconstruct_where({'or', Clause1, Clause2}) ->
    P1 = deconstruct_where(Clause1),
    P2 = deconstruct_where(Clause2),
    #condition{ op = 'or', args = [ P1, P2 ] };
deconstruct_where({'and', Clause1, Clause2}) ->
    P1 = deconstruct_where(Clause1),
    P2 = deconstruct_where(Clause2),
    #condition{ op = 'and', args = [ P1, P2 ] }.
