%% Simple filter predicate benchmark
%% It's recommended to call "error_logger:tty(false)." in the shell first

-module(filter_benchmark).

-export([bench_all/0, bench1/0, bench2/0, bench3/0]).

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

bench2() ->
	FunStr = "fun({X, Y, Z}) -> abs(X) * abs(Y) > abs(Z) end",
	MatchSpec = [{{'$1','$2','$3'}, [], [{'>',{'*',{abs,'$1'},{abs,'$2'}},{abs,'$3'}}]}],
	Event1 = {10, 10, 1000},
	run_bench(FunStr, MatchSpec, Event1, ?MILLION),

	Event2 = {-10, -10, -10},
	run_bench(FunStr, MatchSpec, Event2, ?MILLION),
	ok.

bench3() ->
	FunStr = "fun({X, Y, Z}) when is_integer(X), is_integer(Y), is_list(Z) -> (((X + Y) * hd(Z)) > ((X - Y) div hd(tl(Z)))) or (abs(X) > abs(Y)) end",
	MatchSpec = [{{'$1','$2','$3'}, [{is_integer,'$1'},{is_integer,'$2'},{is_list,'$3'}],
		[{'or',{'>',{'*',{'+','$1','$2'},{hd,'$3'}}, {'div',{'-','$1','$2'},{hd,{tl,'$3'}}}},
         {'>',{abs,'$1'},{abs,'$2'}}}]}],
	Event1 = {10, 20, [0, -5, 1, 2, 3, 1000, 1000000]},
	run_bench(FunStr, MatchSpec, Event1, ?MILLION),

	Event2 = {10, 20, [0, 5, 1, 2, 3, 1000, 1000000]},
	run_bench(FunStr, MatchSpec, Event2, ?MILLION),
	
	Event3 = {100, 10, [0, 5, 1, 2, 3, 1000, 1000000]},
	run_bench(FunStr, MatchSpec, Event3, ?MILLION),
	ok.

bench_all() ->
	bench1(),
	bench2(),
	bench3(),
	ok.