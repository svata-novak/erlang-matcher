%% Benchmark for the second variant of the join predicate
%% It's recommended to call "error_logger:tty(false)." in the shell first

%% The benchmarks usually have to sequentially traverse the whole table.
%% bench3/0 might be much faster however, since it tests whether the first
%% item in the tuple is equal to some constant value

-module(join2_benchmark).

-export([bench1/0, bench2/0, bench3/0, bench_all/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

-define(TIMEOUT, 1000).

send_msgs(Server, Messages) ->
	[gen_server:cast(Server, Message) || Message <- Messages],
	ok.

%send_msgs(_Server, []) ->
%	ok;

%send_msgs(Server, [FirstMessage | Rest]) ->
%	gen_server:cast(Server, FirstMessage),
%	send_msgs(Server, Rest),
%	ok.

generate_increasing_messages(Item, Count, Acc) ->
	case Count > 0 of
		true ->
			{Tag, {X, Y, Z}} = Item,
			generate_increasing_messages({Tag, {X + 1, Y + 1, Z + 1}},
				Count - 1, [Item | Acc]);
		_ -> Acc
	end.

generate_increasing_messages(Item, Count) ->
	lists:reverse(generate_increasing_messages(Item, Count, [])).

generate_increasing_messages2(Item, Count, Acc) ->
	case Count > 0 of
		true ->
			{Tag, {X, Y, Z}} = Item,
			generate_increasing_messages2({Tag, {X + 1, Y + 1, Z}},
				Count - 1, [Item | Acc]);
		_ -> Acc
	end.

generate_increasing_messages3(Item, Count) ->
	lists:reverse(generate_increasing_messages3(Item, Count, [])).

generate_increasing_messages3(Item, Count, Acc) ->
	case Count > 0 of
		true ->
			{Tag, {X, Y, Z}} = Item,
			generate_increasing_messages3({Tag, {X, Y + 1, Z + 1}},
				Count - 1, [Item | Acc]);
		_ -> Acc
	end.

generate_increasing_messages2(Item, Count) ->
	lists:reverse(generate_increasing_messages2(Item, Count, [])).

generate_identical_messages(Item, Count) ->
	lists:duplicate(Count, Item).

run_bench(MatchSpecStub, FirstTableItem, FirstEvent, TableItemCount, EventCount,
	MsgGenFunction1, MsgGenFunction2) ->
	{ok, Server} = join2:start(MatchSpecStub, undefined, ?TIMEOUT),
	TableMessages = erlang:apply(MsgGenFunction1, [FirstTableItem, TableItemCount]),
	gen_server:call(Server, {start_benchmark, false, self()}),
	send_msgs(Server, TableMessages),
	ElapsedTime1 = receive
		ElTime1 -> ElTime1
	end,
	
	io:format("RESULTS:~n"),
	io:format("MatchSpecStub: ~p~nFirst table item: ~p~nTable item count: ~p~n",
		[MatchSpecStub, FirstTableItem, TableItemCount]),
	benchmark:print_elapsed(ElapsedTime1, TableItemCount),
	io:format("--------------------------------------------------------------------------------~n"),	

	EventMessages = erlang:apply(MsgGenFunction2, [FirstEvent, EventCount]),
	gen_server:call(Server, {start_benchmark, true, self()}),
	send_msgs(Server, EventMessages),
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
		fun generate_increasing_messages/2, fun generate_increasing_messages/2).

bench1() ->
	MatchSpecStub = [{{'$1','$2','$3'},[{'>','$1','$4'}],['$_']}],
	FirstItem = {1, 1, 1},
	FirstTableItem = {table, FirstItem},
	FirstEvent = {event, FirstItem},

	io:format("50 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, FirstEvent, 100, 100000),
	run_bench_increasing(MatchSpecStub, FirstTableItem, FirstEvent, 10000, 1000),

	io:format("no matches~n"),
	NotMatchingEvent = {event, {1000000, 1000000, 1000000}},
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 100, 100000),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 10000, 10000),

	io:format("100 items in table, 100000 events, 100 % of events match~n"),
	AlwaysMatchingEvent = {event, {-1000000, -1000000, -1000000}},
	run_bench_increasing(MatchSpecStub, FirstTableItem, AlwaysMatchingEvent, 100, 100000),
	io:format("100 items in table, 10000 events, 0 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 100, 100000),

	io:format("10000 items in table, 10000 events, 0 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, NotMatchingEvent, 10000, 10000),
	io:format("10000 items in table, 1000 events, 100 % of events match~n"),
	run_bench_increasing(MatchSpecStub, FirstTableItem, AlwaysMatchingEvent, 10000, 1000),
	
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
	run_bench(MatchSpecStub, FirstTableItem, Event, 100, 10000,
		fun generate_increasing_messages2/2, fun generate_identical_messages/2),
	run_bench(MatchSpecStub, FirstTableItem, Event, 10000, 1000,
		fun generate_increasing_messages2/2, fun generate_identical_messages/2),
	
	io:format("No events match~n"),
	FirstTableItem2 = {table, {1, 1, [1, 2, 3, 4, 5]}},
	Event2 = {event, {?MILLION, ?MILLION, [1, 2, 3, 4, 5]}},
	run_bench(MatchSpecStub, FirstTableItem2, Event2, 100, 10000,
		fun generate_increasing_messages2/2, fun generate_identical_messages/2),
	run_bench(MatchSpecStub, FirstTableItem2, Event2, 10000, 1000,
		fun generate_increasing_messages2/2, fun generate_identical_messages/2),
	ok.

bench3() ->
	%MatchSpecStub = [{{'$1','$2','$3'},[{'=:=','$1','$4'}],['$_']}],
	MatchSpecStub = [{{'$4','$2','$3'},[],['$_']}],
	FirstItem = {1, 1, 1},
	FirstTableItem = {table, FirstItem},
	FirstEvent = {event, FirstItem},

	io:format("Every event matches one item, key is bound"),
	run_bench(MatchSpecStub, FirstTableItem, FirstEvent, 100, 10000,
		fun generate_increasing_messages/2, fun generate_increasing_messages/2),
	run_bench(MatchSpecStub, FirstTableItem, FirstEvent, 10000, 10000,
		fun generate_increasing_messages/2, fun generate_increasing_messages/2),

	io:format("Events don't match any items, key is bound"),
	FirstEvent2 = {event, {?MILLION, 1, 1}},
	run_bench(MatchSpecStub, FirstTableItem, FirstEvent2, 100, 10000,
		fun generate_increasing_messages/2, fun generate_increasing_messages/2),
	run_bench(MatchSpecStub, FirstTableItem, FirstEvent2, 10000, 10000,
		fun generate_increasing_messages/2, fun generate_increasing_messages/2),
	
	ok.

bench_all() ->
	bench1(),
	bench2(),
	bench3(),
	ok.