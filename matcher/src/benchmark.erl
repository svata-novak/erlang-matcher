-module(benchmark).

-export([run_bench/5, print_elapsed/2, test_avg/4]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

-define(TIMEOUT, 1000).

repeat(Server, Event, Repeats) ->
	case Repeats > 0 of
		true ->
			gen_server:cast(Server, {event, Event}),
			repeat(Server, Event, Repeats - 1);
		_ -> ok
	end.

print_elapsed(ElapsedTime, Repeats) ->
	io:format("total time elapsed [s];elapsed time per event [us];events per second~n"),
	%io:format("Elapsed time: ~p us = ~p ms = ~p s~n",
	%		  [ElapsedTime, ElapsedTime / ?THOUSAND, ElapsedTime / ?MILLION]),
	AverageEventElapsed = ElapsedTime / Repeats,
	%io:format("Average elapsed time per one event: ~p us = ~p ms = ~p s~n",
	%		  [AverageEventElapsed, AverageEventElapsed / ?THOUSAND,
	%		   AverageEventElapsed / ?MILLION]),
	EventCount = 1 / AverageEventElapsed,
	io:format("~p;~p;~p~n", [ElapsedTime / ?MILLION, AverageEventElapsed, EventCount * ?MILLION]),
	%io:format("Events per s: ~p (per ms: ~p)~n", [EventCount * ?MILLION, EventCount * ?THOUSAND]),
	ok.

% set up the predicate, send Event Repeats times, print information
% including the elapsed time
run_bench(PredicateModule, FunStr, MatchSpec, Event, Repeats) ->
	%{ok, Server} = filter:start(MatchSpec, undefined, ?TIMEOUT),
	{ok, Server} = erlang:apply(PredicateModule, start, [MatchSpec, undefined, ?TIMEOUT]),
	gen_server:call(Server, {start_benchmark, self()}),
	repeat(Server, Event, Repeats),
	ElapsedTime = receive
		ElTime -> ElTime
	end,
	io:format("RESULTS:~n"),
	io:format("Fun: ~p~nEvent: ~p~n", [FunStr, Event]),
	print_elapsed(ElapsedTime, Repeats),
	io:format("================================================================================~n"),
	ok.

test_avg(M, F, A, N) when N > 0 ->
    L = test_loop(M, F, A, N, []),
    Length = length(L),
    Min = lists:min(L),
    Max = lists:max(L),
    Med = lists:nth(round((Length / 2)), lists:sort(L)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, L) / Length),
    io:format("Range: ~b - ~b mics~n"
          "Median: ~b mics~n"
          "Average: ~b mics~n",
          [Min, Max, Med, Avg]),
    Med.
 
test_loop(_M, _F, _A, 0, List) ->
    List;
test_loop(M, F, A, N, List) ->
    {T, _Result} = timer:tc(M, F, A),
    test_loop(M, F, A, N - 1, [T|List]).
