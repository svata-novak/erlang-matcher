%% Simple join predicate benchmark
%% It's recommended to call "error_logger:tty(false)." in the shell first

-module(join_benchmark).

-export([bench_all/0, bench1/0, bench2/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

-define(TIMEOUT, 1000).

send_message(Message, Repeats, Server) ->
	case Repeats > 0 of
		true ->
			gen_server:cast(Server, Message),
			send_message(Message, Repeats - 1, Server);
		_ -> ok
	end.

process_sub_rules([], _Server) ->
	ok;

process_sub_rules([CurrentSubRule | Rest], Server) ->
	{Repeats, Message} = CurrentSubRule,
	send_message(Message, Repeats, Server),
	process_sub_rules(Rest, Server).

process_sub_rules(SubRules, Repeats, Server) ->
	case Repeats > 0 of
		true ->
			process_sub_rules(SubRules, Server),
			process_sub_rules(SubRules, Repeats - 1, Server);
		_ -> ok
	end.

process_send_rule([], _Server) ->
	ok;

process_send_rule([CurrentRule | Rest], Server) ->
	{Repeats, SubRules} = CurrentRule,
	process_sub_rules(SubRules, Repeats, Server),
	process_send_rule(Rest, Server).

run_bench(FunStr, MatchSpec, SendRule, Repeats) ->
	{ok, Server} = join:start(MatchSpec, undefined, ?TIMEOUT),
	gen_server:call(Server, {start_benchmark, self()}),
	process_send_rule(SendRule, Server),
	ElapsedTime = receive
		ElTime -> ElTime
	end,
	io:format("RESULTS:~n"),
	io:format("Fun: ~p~nSendRule: ~p~n", [FunStr, SendRule]),
	benchmark:print_elapsed(ElapsedTime, Repeats),
	io:format("================================================================================~n"),
	ok.

alternating_rule(Event1, Event2, FirstLastCount, Repeats) ->
	InitialSubRule = {1, [{FirstLastCount, {left, Event1}}]},
	MainSubRule = {Repeats, [{FirstLastCount * 2, {right, Event2}}, {FirstLastCount * 2, {left, Event1}}]},
	LastSubRule = {1, [{FirstLastCount, {right, Event2}}]},
	[InitialSubRule, MainSubRule, LastSubRule].

bench1() ->
	FunStr = "fun({A, B, C, D, E, F}) -> A - D > 1000 end",
	MatchSpec = [{{'$1','$2','$3','$4','$5','$6'}, [], [{'>',{'-','$1','$4'},1000}]}],
	Event1 = {10000, 0, 0},
	Event2 = {1000, 0, 0},
	% 1 million + 2 events (3 left events, then 3 right events,
	% then 3 left events, then 3 right events, then 3 left events, ...
	% then 3 right events)
	%InitialSubRule = {1, [{3, {left, Event1}}]}, % send once 3 left events
	% do 83333 times the following: send 6 right events, then send 6 left events
	%MainSubRule = {83333, [{6, {right, Event2}}, {6, {left, Event1}}]},
	%LastSubRule = {1, [{3, {right, Event2}}]}, % send once 3 right events
	%SendRule = [InitialSubRule, MainSubRule, LastSubRule],
	SendRule = alternating_rule(Event1, Event2, 3, 83333),
	run_bench(FunStr, MatchSpec, SendRule, ?MILLION + 2),

	% 500 000 left events, then 500 000 matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event1}},
		{?MILLION / 2, {right, Event2}}]}], ?MILLION),

	% same as the first benchmark, but the events don't match
	run_bench(FunStr, MatchSpec, alternating_rule(Event1, Event1, 3, 83333),
		?MILLION + 2),

	% 500 000 left events, then 500 000 non-matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event1}},
		{?MILLION / 2, {right, Event1}}]}], ?MILLION),
	ok.

bench2() ->
	FunStr = "fun({A, B, C, D, E, F}) when is_integer(A), is_integer(B), "
		"is_list(C), is_integer(D), is_integer(E), is_list(F) -> "
		"((abs(A) * B) > (abs(D) * E)) or ((hd(tl(C)) div hd(tl(F))) > 2) end",
	MatchSpec = [{{'$1','$2','$3','$4','$5','$6'},
		[{is_integer,'$1'}, {is_integer,'$2'}, {is_list,'$3'},
		{is_integer,'$4'}, {is_integer,'$5'}, {is_list,'$6'}],
		[{'or',{'>',{'*',{abs,'$1'},'$2'},{'*',{abs,'$4'},'$5'}},
         {'>',{'div',{hd,{tl,'$3'}},{hd,{tl,'$6'}}},2}}]}],
	Event1 = {10, 10, [1, 2, 3, 4, 5]},
	Event2 = {1, 1, [5, 4, 3, 2, 1]},

	run_bench(FunStr, MatchSpec, alternating_rule(Event1, Event2, 3, 83333),
		?MILLION + 2), % events match

	run_bench(FunStr, MatchSpec, alternating_rule(Event1, Event1, 3, 83333),
		?MILLION + 2), % events don't match
	
	% 500 000 left events, then 500 000 matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event1}},
		{?MILLION / 2, {right, Event2}}]}], ?MILLION),

	% 500 000 left events, then 500 000 non-matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event1}},
		{?MILLION / 2, {right, Event1}}]}], ?MILLION),

	Event3 = {-5, -5, [5000, 4000, 3000, 2000, 1000]},
	Event4 = {-5, -5, [5, 4, 3, 2, 1]},

	run_bench(FunStr, MatchSpec, alternating_rule(Event3, Event4, 3, 83333),
		?MILLION + 2), % events match

	run_bench(FunStr, MatchSpec, alternating_rule(Event3, Event3, 3, 83333),
		?MILLION + 2), % events don't match
	
	% 500 000 left events, then 500 000 matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event3}},
		{?MILLION / 2, {right, Event4}}]}], ?MILLION),

	% 500 000 left events, then 500 000 non-matching right events
	run_bench(FunStr, MatchSpec, [{1, [{?MILLION / 2, {left, Event3}},
		{?MILLION / 2, {right, Event3}}]}], ?MILLION),
	ok.

bench_all() ->
	bench1(),
	bench2(),
	ok.