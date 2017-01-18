-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {tag :: atom(),
                name :: string(),
                start = erlang:system_time(milli_seconds)}).

init([Tag, SubQ]) when is_atom(Tag), not is_list(SubQ) ->
    {Mod, _Args} = SubQ,
    Name = Mod:describe(),
    {ok, #state{name = Name, tag = Tag}, SubQ}.

describe(_) ->
    "debug".

start(_, State) ->
    {ok, State}.

emit(_Child, Data, State = #state{name = Name, tag = Tag}) ->
    dqe_lib:pdebug(Tag, "~p emit ~n", [Name]),
    {emit, Data, State}.

done({last, _Child}, State = #state{start = Start, name = Name, tag = Tag}) ->
    Diff  = Start - erlang:system_time(milli_seconds),
    dqe_lib:pdebug(Tag, "~p finished after ~pms.~n", [Name, Diff]),
    {done, State}.
