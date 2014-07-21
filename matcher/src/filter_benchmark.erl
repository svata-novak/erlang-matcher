-module(filter_benchmark).

-export([bench1/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

run_bench(FunStr, MatchSpec, Event, Repeats) ->
	benchmark:run_bench(filter, FunStr, MatchSpec, Event, Repeats).

bench1() ->
	FunStr = "fun({X, Y, Z}) -> X > 1000 end",
	MatchSpec = [{{'$1','$2','$3'},[],[{'>','$1',1000}]}],
	Event1 = {10000, 0, 0},
	run_bench(FunStr, MatchSpec, Event1, ?MILLION),
	
	Event2 = {0, 0, 0},
	run_bench(FunStr, MatchSpec, Event2, ?MILLION),
	ok.