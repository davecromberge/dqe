-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {tag :: atom(),
                mod :: string(),
                start = erlang:system_time(milli_seconds)}).

init([Tag, SubQ]) when is_atom(Tag), not is_list(SubQ) ->
    {Mod, _Args} = SubQ,
    {ok, #state{mod = Mod, tag = Tag}, SubQ}.

describe(_) ->
    "debug".

start(_, State) ->
    {ok, State}.

emit(_Child, Data, State = #state{mod = Mod, tag = Tag}) ->
    dqe_lib:pdebug(Tag, "~p emit ~n", [Mod]),
    {emit, Data, State}.

done({last, _Child}, State = #state{start = Start, mod = Mod, tag = Tag}) ->
    Diff  = erlang:system_time(milli_seconds) - Start,
    dqe_lib:pdebug(Tag, "~p finished after ~pms.~n", [Mod, Diff]),
    {done, State}.
