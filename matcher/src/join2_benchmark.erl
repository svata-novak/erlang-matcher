%% Benchmark for the second variant of the join predicate
%% It's recommended to call "error_logger:tty(false)." in the shell first

%% WARNING: Since the "loops" in this benchmark are very slow
%% (except for send_identical_items/3, which just sends the same message),
%% only some of the data produced by bench2/0 are useful.
%% These data suggest that processing one message (event) on the server takes
%% about one or two us (or less, depending mainly on the CPU)

%% Possible fix: generate all the benchmark data (messages)
%% in advance and then send them

-module(join2_benchmark).

-export([bench1/0, bench2/0, bench_all/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

-define(TIMEOUT, 1000).

send_items(Server, Item, RemainingItems) ->
	case RemainingItems > 0 of
		true ->
			{Tag, {X, Y, Z}} = Item,
			gen_server:cast(Server, {table, Item}),
			send_items(Server, {Tag, {X + 1, Y + 1, Z + 1}}, RemainingItems - 1),
			ok;
		_ -> ok
	end,
	ok.

run_bench(MatchSpecStub, FirstTableItem, FirstEvent, TableItemCount, EventCount,
	SendFunction1, SendFunction2) ->
	{ok, Server} = join2:start(MatchSpecStub, undefined, ?TIMEOUT),
	gen_server:call(Server, {start_benchmark, false, self()}),
	%send_items(Server, FirstTableItem, TableItemCount),
	erlang:apply(SendFunction1, [Server, FirstTableItem, TableItemCount]),
	ElapsedTime1 = receive
		ElTime1 -> ElTime1
	end,
	
	io:format("RESULTS:~n"),
	io:format("MatchSpecStub: ~p~nFirst table item: ~p~nTable item count: ~p~n",
		[MatchSpecStub, FirstTableItem, TableItemCount]),
	benchmark:print_elapsed(ElapsedTime1, TableItemCount),
	io:format("--------------------------------------------------------------------------------~n"),	

	gen_server:call(Server, {start_benchmark, true, self()}),
	%send_items(Server, FirstEvent, EventCount),
	erlang:apply(SendFunction2, [Server, FirstEvent, EventCount]),
	ElapsedTime2 = receive
		ElTime2 -> ElTime2
	end,

	io:format("First event: ~p~nEvent count: ~p~n", [FirstEvent, EventCount]),
	benchmark:print_elapsed(ElapsedTime2, EventCount),
	io:format("================================================================================~n"),
	ok.

run_bench_increasing(MatchSpecStub, FirstTableItem, FirstEvent, TableItemCount,
	EventCount) ->
	run_bench(MatchSpecStub, FirstTableItem, FirstEvent, TableItemCount, EventCount,
		fun send_items/3, fun send_items/3).

bench1() ->
	MatchSpecStub = [{{'$1','$2','$3'},[{'>','$1','$4'}],['$_']}],
	FirstItem = {1, 1, 1},
	FirstTableItem = {table, FirstItem},
	FirstEvent = {event, FirstItem},

	io:format("50 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, FirstEvent, 100, 10000),
	run_bench_increasing(MatchSpecStub, FirstTableItem, FirstEvent, 10000, 10000),

	io:format("no matches~n"),
	NotMatchingEvent = {event, {1000000, 1000000, 1000000}},
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 100, 100),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 10000, 10000),

	io:format("100 items in table, 10000 events, 100 % of events match~n"),
	AlwaysMatchingEvent = {event, {-1000000, -1000000, -1000000}},
	run_bench_increasing(MatchSpecStub, FirstTableItem, AlwaysMatchingEvent, 100, 10000),
	io:format("100 items in table, 10000 events, 0 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 100, 10000),

	io:format("10000 items in table, 10000 events, 0 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 10000, 10000),
	io:format("10000 items in table, 10000 events, 100 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, AlwaysMatchingEvent, 10000, 10000),
	
	ok.

send_items2(Server, Item, RemainingItems) ->
	case RemainingItems > 0 of
		true ->
			{Tag, {X, Y, Z}} = Item,
			gen_server:cast(Server, {table, Item}),
			send_items2(Server, {Tag, {X + 1, Y + 1, Z}}, RemainingItems - 1),
			ok;
		_ -> ok
	end,
	ok.

send_identical_items(Server, Item, RemainingItems) ->
	case RemainingItems > 0 of
		true ->
			gen_server:cast(Server, {table, Item}),
			send_identical_items(Server, Item, RemainingItems - 1),
			ok;
		_ -> ok
	end,
	ok.

bench2() ->
	MatchSpecStub =
		[{{'$1','$2','$3'}, [{is_integer,'$1'}, {is_integer,'$2'}, {is_list,'$3'},
		{is_integer,'$4'}, {is_integer,'$5'}, {is_list,'$6'},
		{'or',{'>',{'*',{abs,'$1'},'$2'},{'*',{abs,'$4'},'$5'}},
		{'>',{'div',{hd,{tl,'$3'}},{hd,{tl,'$6'}}},2}}], ['$_']}],
	FirstTableItem = {table, {?MILLION, ?MILLION, [1, 2, 3, 4, 5]}},
	Event = {event, {1, 1, [5, 4, 3, 2, 1]}},

	io:format("All events match~n"),
	run_bench(MatchSpecStub, FirstTableItem, Event, 100, ?MILLION, fun send_items2/3,
		fun send_identical_items/3),
	run_bench(MatchSpecStub, FirstTableItem, Event, 10000, ?MILLION, fun send_items2/3,
		fun send_identical_items/3),
	
	io:format("No events match~n"),
	FirstTableItem2 = {table, {1, 1, [1, 2, 3, 4, 5]}},
	Event2 = {event, {?MILLION, ?MILLION, [1, 2, 3, 4, 5]}},
	run_bench(MatchSpecStub, FirstTableItem2, Event2, 100, ?MILLION, fun send_items2/3,
		fun send_identical_items/3),
	run_bench(MatchSpecStub, FirstTableItem2, Event2, 10000, ?MILLION, fun send_items2/3,
		fun send_identical_items/3),
	ok.

bench_all() ->
	bench1(),
	bench2(),
	ok.