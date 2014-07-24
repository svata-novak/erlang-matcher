-module(join2_test).

-export([string_replace/2, test1/2, test1_comp/2, test1_bench/0, replace/2, test2_bench/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

string_replace(MatchSpecParts, {X, Y, Z}) ->
	lists:reverse(lists:foldl(
		fun(Part, Acc) ->
			Res = case Part of
				'$4' -> X;
				'$5' -> Y;
				'$6' -> Z;
				_ -> Part
			end,
			[Res | Acc]
			end, "", MatchSpecParts)).

test1(MatchSpecParts, {X, Y, Z}) ->
	Str = lists:flatten(string_replace(MatchSpecParts, {X, Y, Z})),
	{ok, Tokens, _} = erl_scan:string(Str),
	{ok, [Form]} = erl_parse:parse_exprs(Tokens),
	{_, MatchSpec, _} = erl_eval:expr(Form, []),
	MatchSpec.

test1_comp(MatchSpecParts, {X, Y, Z}) ->
	MatchSpec = test1(MatchSpecParts, {X, Y, Z}),
	ets:match_spec_compile(MatchSpec).

repeat(MatchSpecParts, Repeats, FirstItem) ->
	case Repeats > 0 of
		true ->
			test1_comp(MatchSpecParts, {integer_to_list(FirstItem), 0, 0}),
			repeat(MatchSpecParts, Repeats - 1, FirstItem + 1);
		false -> ok
	end.

test1_bench() ->
	StartTime = erlang:now(),
	io:format("Start: ~p~n", [StartTime]),
	MatchSpecParts = ["[{{'$1'},[],[{'>','$1',", '$4', "}]}]."],
	Repeats = 100000,
	repeat(MatchSpecParts, Repeats, 1),
	FinishTime = erlang:now(),
	io:format("Finish: ~p~n", [FinishTime]),
	ElapsedTime = timer:now_diff(FinishTime, StartTime),
	io:format("Elapsed time: ~p us = ~p ms = ~p s~n",
			  [ElapsedTime, ElapsedTime / ?THOUSAND, ElapsedTime / ?MILLION]),
	AverageEventElapsed = ElapsedTime / Repeats,
	io:format("Average elapsed time per one event: ~p us = ~p ms = ~p s~n",
			  [AverageEventElapsed, AverageEventElapsed / ?THOUSAND,
			   AverageEventElapsed / ?MILLION]),
	ok.

replace_list(Token, {X, Y, Z}) ->
	lists:map(fun(Item) -> replace(Item, {X, Y, Z}) end, Token).

replace_tuple(Token, {X, Y, Z}) ->
	erlang:list_to_tuple(replace_list(erlang:tuple_to_list(Token), {X, Y, Z})).

replace(Token, {X, Y, Z}) ->
	case is_list(Token) of
		true -> replace_list(Token, {X, Y, Z});
		_ ->
			case is_tuple(Token) of
				true -> replace_tuple(Token, {X, Y, Z});
				_ ->
					case Token of
						'$4' -> X;
						'$5' -> Y;
						'$6' -> Z;
						_ -> Token
					end
 			end
	end.

test2(MatchSpec, {X, Y, Z}) ->
	(replace(MatchSpec, {X, Y, Z})).

repeat2(MatchSpec, Repeats, FirstItem) ->
	case Repeats > 0 of
		true ->
			test2(MatchSpec, {integer_to_list(FirstItem), 0, 0}),
			repeat2(MatchSpec, Repeats - 1, FirstItem + 1);
		false -> ok
	end.

test2_bench() ->
	StartTime = erlang:now(),
	io:format("Start: ~p~n", [StartTime]),
	MatchSpec = [{{'$1'},[],[{'>','$1','$4'}]}],
	Repeats = 100000,
	repeat2(MatchSpec, Repeats, 1),
	FinishTime = erlang:now(),
	io:format("Finish: ~p~n", [FinishTime]),
	ElapsedTime = timer:now_diff(FinishTime, StartTime),
	io:format("Elapsed time: ~p us = ~p ms = ~p s~n",
			  [ElapsedTime, ElapsedTime / ?THOUSAND, ElapsedTime / ?MILLION]),
	AverageEventElapsed = ElapsedTime / Repeats,
	io:format("Average elapsed time per one event: ~p us = ~p ms = ~p s~n",
			  [AverageEventElapsed, AverageEventElapsed / ?THOUSAND,
			   AverageEventElapsed / ?MILLION]),
	ok.