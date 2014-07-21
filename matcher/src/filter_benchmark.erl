-module(filter_benchmark).

-export([bench1/2]).

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
	io:format("Start: ~p~n", [erlang:now()]),
	repeat(Server, Event1, Repeats),
	receive
		_ -> io:format("Finish: ~p~n", [erlang:now()])
	end,
	%benchmark_util:test_avg(gen_server, cast, [Server, {event, Event1}], 10000000),
	ok.