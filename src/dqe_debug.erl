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
    dqe_lib:pdebug('debug', "~p ~p~n", [Child,
                                  mmath_bin:to_list(mmath_bin:derealize(Data))]),
    {emit, Data, State}.

done({last, Child}, State = #state{start = Start}) ->
    Diff  = Start - erlang:system_time(milli_seconds),
    dqe_lib:pdebug('debug', "Finished after ~pms.~n", [Child, Diff]),
    {done, State}.
