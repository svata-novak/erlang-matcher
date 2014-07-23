%% A join predicate
%%
%% Similar to the filter predicate, but events can be of one of two types:
%% "left" or "right"
%%
%% Example: If a predicate match spec corresponds to the function
%% "fun({A, B, C, D, E, F}) -> A - D > 1000 end", then a
%% "left" event which equals to {A, B, C} matches the predicate
%% (causing the notify/4 function to be called) if the "right" event at the top
%% of the corresponding (i.e. right) queue equals to {D, E, F} and the value
%% of A minus the value of D is greater than 1000. Similarly for a "right"
%% event. After match test, both events are discarded (it doesn't matter what
%% the match result is). If there was no event in the other queue, the incoming
%% event is added to its queue and no other action (matching) is performed.
%% The implication is that at least one of the two queues is
%% empty at any given time.

%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = join:start([{{'$1','$2','$3','$4','$5','$6'}, [], [{'>',{'-','$1','$4'},1000}]}], Ref, infinity).
%% gen_server:cast(Server, {left, {10000, 0, 0}}).
%% gen_server:cast(Server, {right, {1000, 0, 0}}).
%% (notify is called)
%% ...

%% TODO - implement the notify/4 function

-module(join).

-behaviour(gen_server).

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {predicate_ms, left_queue, right_queue, callback,
				benchmark_callback, benchmark_start_time, timeout}).

start(PredicateMS, Callback, TimeOut) ->
	gen_server:start(?MODULE, [PredicateMS, Callback, TimeOut], []).

start_link(PredicateMS, Callback, TimeOut) ->
	gen_server:start_link(?MODULE, [PredicateMS, Callback, TimeOut], []).

init([PredicateMS, Callback, TimeOut]) ->
	{ok, #state{predicate_ms=ets:match_spec_compile(PredicateMS),
				left_queue=queue:new(), right_queue=queue:new(),
				callback=Callback, timeout=TimeOut}, TimeOut}.

% set benchmark related information, store the current time
handle_call({start_benchmark, BenchmarkCallback}, _From, State) ->
	TimeOut = State#state.timeout,
	{reply, ok, State#state{benchmark_callback=BenchmarkCallback,
							benchmark_start_time=erlang:now()}, TimeOut};

handle_call(_E, _From, State) ->
    {reply, ok, State}.

% not implemented yet
notify(_Callback, _LeftEvent, _RightEvent, _LastEvent, _PredicateMS) ->
	ok.

%% check if the two events match the predicate
%% return true if they do, false otherwise
match(PredicateMS, LeftEvent, RightEvent, LastEvent, Callback) ->
	{A, B, C} = LeftEvent,
	{D, E, F} = RightEvent,
	case ets:match_spec_run([{A, B, C, D, E, F}], PredicateMS) of
		[true] -> notify(Callback, LeftEvent, RightEvent, LastEvent, PredicateMS), true;
		_ -> false
	end.

%% handle "left" event
handle_cast({left, LeftEvent}, State) ->
	LeftQueue = State#state.left_queue,
	RightQueue = State#state.right_queue,
	PredicateMS = State#state.predicate_ms,
	Callback = State#state.callback,
	TimeOut = State#state.timeout,
	
	NewState = case queue:out(RightQueue) of
		{empty, _} -> State#state{left_queue=queue:in(LeftEvent, LeftQueue)};
		{{value, RightEvent}, NewRightQueue} ->
			match(PredicateMS, LeftEvent, RightEvent, left, Callback),
			State#state{right_queue=NewRightQueue}
	end,
	{noreply, NewState, TimeOut};

%% handle "right" event
%% (code is duplicated, but there are no redundant comparisons)
%% TODO - maybe extract the common code into a separate method
%% with parameters like CurrentQueue, OtherQueue...
handle_cast({right, RightEvent}, State) ->
	LeftQueue = State#state.left_queue,
	RightQueue = State#state.right_queue,
	PredicateMS = State#state.predicate_ms,
	Callback = State#state.callback,
	TimeOut = State#state.timeout,
	
	NewState = case queue:out(LeftQueue) of
		{empty, _} -> State#state{right_queue=queue:in(RightEvent, RightQueue)};
		{{value, LeftEvent}, NewLeftQueue} ->
			match(PredicateMS, LeftEvent, RightEvent, right, Callback),
			State#state{left_queue=NewLeftQueue}
	end,
	{noreply, NewState, TimeOut};

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
	{stop, timeout, State};

handle_info(_E, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
