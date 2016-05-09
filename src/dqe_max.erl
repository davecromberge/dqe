-module(dqe_max).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2]).

-record(state, {
          time :: pos_integer()
         }).

init([Time]) when is_integer(Time) ->
    #state{time = Time}.

chunk(#state{time = Time}) ->
    Time * ?RDATA_SIZE.

resolution(Resolution, #state{time = Time}) ->
    dqe_time:apply_times(Time, Resolution).

describe(#state{time = Time}) ->
    ["max(", integer_to_list(Time), "ms)"].

spec() ->
    {<<"max">>, [metric, integer], none, metric}.

run([Data], S = #state{time = Time}) ->
    {mmath_aggr:max(Data, Time), S}.