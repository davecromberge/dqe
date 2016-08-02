-module(dqe_mul_comb).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    1.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{}) ->
    ["mul()"].

spec() ->
    {<<"mul">>, [], metric, metric}.

run(Datas, S) ->
    {mmath_comb:mul(Datas), S}.

help() ->
    <<"Combines multiple series into one by multiplying the values for each "
      "time offset together.">>.
