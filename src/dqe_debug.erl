-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {tag :: atom(),
                desc :: string(),
                start = erlang:system_time(milli_seconds)}).

init([Tag, Desc, SubQ]) when is_atom(Tag), not is_list(SubQ) ->
    {ok, #state{tag = Tag, desc = Desc}, SubQ}.

describe(_) ->
    "debug".

start(_, State) ->
    {ok, State}.

emit(_Child, Data, State = #state{desc = Desc, tag = Tag}) ->
    dqe_lib:pdebug(Tag, "~p emit ~n", [Desc]),
    {emit, Data, State}.

done({last, _Child}, State = #state{start = Start, desc = Desc, tag = Tag}) ->
    Diff  = erlang:system_time(milli_seconds) - Start,
    dqe_lib:pdebug(Tag, "~p finished after ~pms.~n", [Desc, Diff]),
    {done, State}.
