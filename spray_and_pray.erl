-module(spray_and_pray).

-export([run/2]).

%%% Includes

-include_lib("stdlib/include/assert.hrl").

%%% Specs

-type callback() :: fun(() -> term())
                  | fun((Context :: term()) -> term())
                  | {Module :: atom(), Function :: atom()}.

-type request() :: #{ callback  := callback(),
                      weight    => pos_integer() }.

-type simulation_options() :: #{ concurrency => pos_integer(),
                                 duration    => pos_integer() }.

-type simulation() :: #{ requests := [request()],
                         context  => term() }.

-spec run(simulation(), simulation_options()) -> ok.

%%% Constants

-define(DEFAULT_CONCURRENCY, 8).
-define(DEFAULT_DURATION, 5000).
-define(DEFAULT_WEIGHT, 1).

%%% API

run(#{ requests := Requests }, Options) ->
    Concurrency = maps:get(concurrency, Options, ?DEFAULT_CONCURRENCY),
    Duration = maps:get(duration, Options, ?DEFAULT_DURATION),
    ?assert(0 < Concurrency),
    ?assert(0 < Duration),

    CPW = compute_concurrency_per_weight(Concurrency, Requests),

    FinishRef = make_ref(),
    timer:send_after(Duration, {finish, FinishRef}),
    run_loop(FinishRef).

%%% Utils

run_loop(FinishRef) ->
    receive
        {finish, FinishRef} ->
            ok;
        Other ->
            io:format(standard_error, "Unhandled message: ~p~n", [Other]),
            run_loop(FinishRef)
    end.

compute_concurrency_per_weight(Concurrency, Requests) ->
    TotalWeight = sum_weights(Requests),
    Concurrency / TotalWeight.

sum_weights(Requests) ->
    lists:foldl(fun(Request, Sum) ->
                    Weight = maps:get(weight, Request, ?DEFAULT_WEIGHT),
                    ?assert(0 < Weight),
                    Sum + Weight
                end, 0, Requests).
