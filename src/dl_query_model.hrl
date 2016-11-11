-type op() :: 'eq' | 'neq' | 'present' | 'and' | 'or'.

-record(condition, {op :: op(),
                    args :: [any()] }).

-type condition() :: #condition{}.

-record(timeframe, { beginning :: binary(),
                     ending    :: binary(),
                     duration  :: binary() }).

-type timeframe() :: #timeframe{}.

-record(selector, { bucket :: binary(),
                    collection :: binary(),
                    metric :: ['*' | binary()],
                    condition :: condition() }).

-type selector() :: #selector{}.

-record(fn, { name :: binary(),
              args :: [term()] }).

-type fn() :: #fn{}.

-record(part, { selector :: selector(),
                timeshift :: binary(),
                fn :: fn(),
                alias :: binary() }).

-type part() :: #part{}.

-record(query, { parts :: [part()],
                 beginning :: binary(),
                 ending :: binary(),
                 duration :: binary() }).

-type query() :: #query{}.

-type lens() :: {Get :: fun(), Put :: fun()}.


