-module(filter_benchmark).

-export([bench1/2]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

repeat(Server, Event, Repeats) ->
	case Repeats > 0 of
		true ->
			gen_server:cast(Server, {event, Event}),
			repeat(Server, Event, Repeats - 1);
		_ -> ok
	end.

%% filter_benchmark:bench1(fun({A}) -> A > 1000 end, 10000000).
bench1(Fun, Repeats) ->
	{ok, Server} = filter:start(Fun, [size], undefined),
	Event1 = orddict:store(size, 500, orddict:new()),
	gen_server:call(Server, {set_benchmark_data, {self(), Repeats}}, infinity),
	StartTime = erlang:now(),
	io:format("Start: ~p~n", [StartTime]),
	repeat(Server, Event1, Repeats),
	FinishTime = receive
		_ ->
			FTime = erlang:now(),
			io:format("Finish: ~p~n", [FTime]),
			FTime
	end,
	ElapsedTime = timer:now_diff(FinishTime, StartTime),
	io:format("Elapsed time: ~p us = ~p ms = ~p s~n",
			  [ElapsedTime, ElapsedTime / ?THOUSAND, ElapsedTime / ?MILLION]),
	AverageEventElapsed = ElapsedTime / Repeats,
	io:format("Average elapsed time per one event: ~p us = ~p ms = ~p s~n",
			  [AverageEventElapsed, AverageEventElapsed / ?THOUSAND,
			   AverageEventElapsed / ?MILLION]),
	%benchmark_util:test_avg(gen_server, cast, [Server, {event, Event1}], 10000000),
	ok.