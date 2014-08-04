-module(join3_benchmark).

-export([bench1/0]).

-define(THOUSAND, 1000).
-define(MILLION, 1000000).

-define(TIMEOUT, 1000).

-record(msg_list_def, {first_message, next_msg_generator, count}).

get_msg_list_def_info(MsgListDef) when is_record(MsgListDef, msg_list_def) ->
	lists:flatten(io_lib:format("First message: ~p~nCount: ~p~n",
		[MsgListDef#msg_list_def.first_message, MsgListDef#msg_list_def.count])).

run_bench(KeyPosition, TableItems, Events, TableItemsInfo, EventsInfo, Description) ->
	{ok, Server} = join3:start(KeyPosition, undefined, ?TIMEOUT),
	io:format("Benchmark description: ~p~n", [Description]),
	io:format("Table items:~n"),
	io:format(TableItemsInfo),
	TableItemsCount = length(TableItems),
	gen_server:call(Server, {start_benchmark, false, self()}),
	benchmark:send_msgs(Server, TableItems),
	ElapsedTime1 = receive
		ElTime1 -> ElTime1
	end,
	
	io:format("RESULTS:~n"),
	benchmark:print_elapsed(ElapsedTime1, TableItemsCount),
	io:format("--------------------------------------------------------------------------------~n"),

	io:format("Events:~n"),
	io:format(EventsInfo),
	EventsCount = length(Events),
	gen_server:call(Server, {start_benchmark, true, self()}),
	benchmark:send_msgs(Server, Events),
	ElapsedTime2 = receive
		ElTime2 -> ElTime2
	end,

	io:format("RESULTS:~n"),
	benchmark:print_elapsed(ElapsedTime2, EventsCount),
	io:format("================================================================================~n"),

	ok.

increase_all({Tag, Tuple}) ->
	{Tag, list_to_tuple(lists:map(fun(X) -> X + 1 end, tuple_to_list(Tuple)))}.

increase_all_except_first({Tag, Tuple}) ->
	[Head | Tail] = tuple_to_list(Tuple),
	{Tag, list_to_tuple([Head | lists:map(fun(X) -> X + 1 end, Tail)])}.

increase_key_every_n_steps(N) ->
	fun ({Tag, Tuple}) ->
		[Head | Tail] = tuple_to_list(Tuple),
		case hd(Tail) =:= N of
			true -> {Tag, list_to_tuple([Head + 1 | lists:duplicate(length(Tail), 1)])};
			_ -> {Tag, list_to_tuple([Head | lists:map(fun(X) -> X + 1 end, Tail)])}
		end
	end.

gen_msgs(MsgListDef) when is_record(MsgListDef, msg_list_def) ->
	benchmark:generate_messages(MsgListDef#msg_list_def.first_message,
		MsgListDef#msg_list_def.next_msg_generator, MsgListDef#msg_list_def.count).

bench1() ->
	% no matches
	TableItems1 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=fun increase_all/1, count=?MILLION},
	Events1 = #msg_list_def{first_message={event, {10 * ?MILLION, 10 * ?MILLION, 10 * ?MILLION}},
							next_msg_generator=fun increase_all/1, count=?MILLION},
	run_bench(1, gen_msgs(TableItems1), gen_msgs(Events1),
		get_msg_list_def_info(TableItems1), get_msg_list_def_info(Events1),
		"no matches, both table items' and events' values increase by 1"),
	
	% every event matches exactly one table item
	Events2 = #msg_list_def{first_message={event, {1,1,1}},
							next_msg_generator=fun increase_all/1, count=?MILLION},
	run_bench(1, gen_msgs(TableItems1), gen_msgs(Events2),
		get_msg_list_def_info(TableItems1), get_msg_list_def_info(Events2),
		"every event matches exactly one table item, both table items' "
		"and events' values increase by 1"),
	
	% every event matches all items
	TableItems3 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=fun increase_all_except_first/1,
							   count=10 * ?THOUSAND},
	Events3 = #msg_list_def{first_message={event, {1,1,1}},
							next_msg_generator=fun increase_all_except_first/1,
							count=10 * ?THOUSAND},
	run_bench(1, gen_msgs(TableItems3), gen_msgs(Events3),
		get_msg_list_def_info(TableItems3), get_msg_list_def_info(Events3),
		"every event matches all items, both table items' and events' "
		"values increase by 1 (except for the first value - the key)"),
	
	% every event matches half of the items
	TableItems41 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=fun increase_all_except_first/1,
							   count=5 * ?THOUSAND},
	TableItems42 = #msg_list_def{first_message={table, {2,1,1}},
							   next_msg_generator=fun increase_all_except_first/1,
							   count=5 * ?THOUSAND},
	run_bench(1, gen_msgs(TableItems41) ++ gen_msgs(TableItems42), gen_msgs(Events3),
		get_msg_list_def_info(TableItems41) ++ get_msg_list_def_info(TableItems42),
		get_msg_list_def_info(Events3),
		"every event matches half of the items, half of the items in table has "
		"key 1, half has key 2, the events have key 1"),

	% every event matches one tenth of the items
	TableItems5 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(1000),
							   count=10 * ?THOUSAND},
	Events5 = #msg_list_def{first_message={event, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(10000),
							   count=100 * ?THOUSAND},
	run_bench(1, gen_msgs(TableItems5), gen_msgs(Events5),
		get_msg_list_def_info(TableItems5), get_msg_list_def_info(Events5),
		"every event matches one tenth of the items (every 1000 values "
		"have the same key)"),

	% every event matches one tenth of the items, 1 million items in the table
	TableItems6 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(10 * ?THOUSAND),
							   count=100 * ?THOUSAND},
	Events6 = #msg_list_def{first_message={event, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(?THOUSAND),
							   count=10 * ?THOUSAND},
	run_bench(1, gen_msgs(TableItems6), gen_msgs(Events6),
		get_msg_list_def_info(TableItems6), get_msg_list_def_info(Events6),
		"every event matches one tenth of the items (every 10 000 values "
		"have the same key)"),

	% every event matches one hundreth of the items
	TableItems7 = #msg_list_def{first_message={table, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(?THOUSAND),
							   count=100 * ?THOUSAND},
	Events7 = #msg_list_def{first_message={event, {1,1,1}},
							   next_msg_generator=increase_key_every_n_steps(?THOUSAND),
							   count=100 * ?THOUSAND},
	run_bench(1, gen_msgs(TableItems7), gen_msgs(Events7),
		get_msg_list_def_info(TableItems7), get_msg_list_def_info(Events7),
		"every event matches one hundreth of the items (every 1000 values "
		"have the same key)"),
	ok.