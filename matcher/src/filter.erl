%% A filter predicate

%% Input arguments:
%%
%% PredicateMS - a match specification that returns true or false
%%
%% Example functions that can be converted to such match specifications
%% using ets:fun2ms/1:
%% fun({X, Y, Z}) -> X > 1000 end
%% (corresponding match specification:
%% [{{'$1','$2','$3'},[],[{'>','$1',1000}]}]
%% )
%%
%% fun({X, Y, Z}) when is_integer(X) -> abs(X) + abs(Y) > 1000 end
%%
%% (corresponding match specification:
%% [{{'$1','$2','$3'},
%%   [{is_integer,'$1'}],
%%   [{'>',{'+',{abs,'$1'},{abs,'$2'}},1000}]}]
%% )
%%
%% Callback - An object that will get a notification
%% if an incoming event matches the predicate
%%
%% TimeOut - how long before handle_info(timeout, State) is called after
%% receiving last message
%% Primarily for benchmark purposes (for sending the elapsed time)
%%
%%
%% Events are represented as tuples of size 3
%%
%% Example: If a predicate match spec corresponds to the function
%% "fun({X, Y, Z}) -> X > 1000 end", then an event matches the predicate
%% (causing the notify/3 function to be called) if the first value of the
%% tuple that representes the event is greater than 1000
%%

%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = filter:start([{{'$1','$2','$3'},[],[{'>','$1',1000}]}], undefined, infinity).
%% gen_server:cast(Server, {event, {500, 0, 0}}).
%% (notify is not called)
%% gen_server:cast(Server, {event, {2000, 0, 0}}).
%% (notify is called)
%% ...

%% TODO - implement the notify/3 function

-module(filter).

-behaviour(gen_server).

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {predicate_ms, callback, benchmark_callback, benchmark_start_time,
				timeout}).

start(PredicateMS, Callback, TimeOut) ->
	gen_server:start(?MODULE, [PredicateMS, Callback, TimeOut], []).

start_link(PredicateMS, Callback, TimeOut) ->
	gen_server:start_link(?MODULE, [PredicateMS, Callback, TimeOut], []).

init([PredicateMS, Callback, TimeOut]) ->
	{ok, #state{predicate_ms=ets:match_spec_compile(PredicateMS),
				callback=Callback, timeout=TimeOut}, TimeOut}.

% set benchmark related information, store the current time
handle_call({start_benchmark, BenchmarkCallback}, _From, State) ->
	TimeOut = State#state.timeout,
	{reply, ok, State#state{benchmark_callback=BenchmarkCallback,
							benchmark_start_time=erlang:now()}, TimeOut};

handle_call(_E, _From, State) ->
    {reply, ok, State}.

% not implemented yet
notify(_Callback, _Event, _PredicateMS) ->
	ok.

% if the event matches the predicate, call notify/3
match(Event, PredicateMS, Callback) when is_tuple(Event) ->
	case ets:match_spec_run([Event], PredicateMS) of
		[true] -> notify(Callback, Event, PredicateMS);
		_ -> ok
	end.

%% handle incoming events, notify the observer if the event matches the predicate
handle_cast({event, Event}, State) when is_record(State, state) ->
	PredicateMS = State#state.predicate_ms,
	Callback = State#state.callback,
	TimeOut = State#state.timeout,
	match(Event, PredicateMS, Callback),
	{noreply, State, TimeOut};

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
