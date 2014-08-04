%% A third variant of a join "predicate"
%%
%% One item in the data tuple is a key (used as a key in the (ets) table)
%% The position of the key is passed to the start(_link)/3 function
%% Messages (events) can be of two types: "table" or "event"
%% Table messages are simply saved to a (ets) table
%% Event messages' data are used to match against the data in the table
%% The matching uses ets:lookup/2 function and is based on the equality test of
%% the key item values (so it's quite fast, unless a lot of items have the same key)

%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = join3:start(1, Ref, infinity).
%% gen_server:cast(Server, {table, {10, 20, 30}}).
%% gen_server:cast(Server, {table, {100, 200, 300}}).
%% gen_server:cast(Server, {table, {10, 10, 10}}).
%% gen_server:cast(Server, {table, {20, 20, 20}}).
%% gen_server:cast(Server, {table, {30, 20, 10}}).
%% gen_server:cast(Server, {event, {10, 20, 30}}).
%% the items in the table which have the same key value
%% (the first item in the tuple) as the event's key value
%% (the first item in the value, 10) are sent to the notify function
%% (specifically: {10, 20, 30} and {10, 10, 10})
%% gen_server:cast(Server, {event, {1000, 20, 10}}).
%% there are no items in the table whose first value is 1000,
%% so notify/3 is not called

%% TODO - implement the notify/3 function

-module(join3).

-behaviour(gen_server).

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TABLE_NAME, events).

-record(state, {key_pos, callback,
				benchmark_callback, benchmark_start_time, timeout, is_last_timeout}).

start(KeyPosition, Callback, TimeOut) ->
	gen_server:start(?MODULE, [KeyPosition, Callback, TimeOut], []).

start_link(KeyPosition, Callback, TimeOut) ->
	gen_server:start_link(?MODULE, [KeyPosition, Callback, TimeOut], []).

init([KeyPosition, Callback, TimeOut]) ->
	ets:new(?TABLE_NAME, [bag, named_table, {keypos, KeyPosition}]),
	{ok, #state{callback=Callback, timeout=TimeOut, key_pos=KeyPosition}}.

% set benchmark related information, store the current time
handle_call({start_benchmark, IsLast, BenchmarkCallback}, _From, State) ->
	TimeOut = State#state.timeout,
	{reply, ok, State#state{benchmark_callback=BenchmarkCallback,
							benchmark_start_time=erlang:now(),
							is_last_timeout=IsLast}, TimeOut};

handle_call(_E, _From, State) ->
    {reply, ok, State}.

% not implemented yet
notify(_Callback, _Event, _Matched) ->
	ok.

match(Event = {_X, _Y, _Z}, KeyPosition, Callback) ->
	Matched = ets:lookup(?TABLE_NAME, element(KeyPosition, Event)),
	case Matched of
		[] -> ok;
		_ -> notify(Callback, Event, Matched)
	end.

handle_cast({table, Event}, State) ->
	ets:insert(?TABLE_NAME, Event),
	{noreply, State, State#state.timeout};

handle_cast({event, Event}, State) ->
	match(Event, State#state.key_pos, State#state.callback),
	{noreply, State, State#state.timeout};

handle_cast(_E, State) ->
    {noreply, State}.

% send the elapsed time to the benchmark process, stop the gen_server
% (it won't be needed again)
handle_info(timeout, State) ->
	BenchmarkEndTime = erlang:now(),
	BenchmarkCallback = State#state.benchmark_callback,
	BenchmarkStartTime = State#state.benchmark_start_time,
	TimeOut = State#state.timeout,
	BenchmarkCallback ! timer:now_diff(BenchmarkEndTime, BenchmarkStartTime) - (TimeOut * 1000),

	case State#state.is_last_timeout of
		true -> {stop, timeout, State};
		_ -> {noreply, State}
	end;

handle_info(_E, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ets:delete(?TABLE_NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.