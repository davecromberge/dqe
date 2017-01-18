-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {start = erlang:system_time(milli_seconds)}).

init([SubQ]) when not is_list(SubQ) ->
    {ok, #state{}, SubQ}.

describe(_) ->
    "debug".

start(_, State) ->
    {ok, State}.

emit(Child, Data, State) ->
    Name = dflow:describe(Child),
    dqe_lib:pdebug('debug', "~p ~p~n", [Name, "emit"]),
    {emit, Data, State}.

done({last, Child}, State = #state{start = Start}) ->
    Name = dflow:describe(Child),
    Diff  = Start - erlang:system_time(milli_seconds),
    dqe_lib:pdebug('debug', "~p Finished after ~pms.~n", [Name, Diff]),
    {done, State}.
