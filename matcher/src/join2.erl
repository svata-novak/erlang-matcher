%% A second variant of a join predicate
%%
%% Messages (events) can be of two types: "table" or "event"
%% Table messages are simply saved to a (ets) table
%% Event messages' data are used to match against the data in the table
%% Predicates are "incomplete" match specifications, which need some data
%% to be filled in before they can be used in the erlang matching functions

%% Example: A predicate "match specification stub" is the following:
%% [{{'$1','$2','$3'},[{'>','$1','$4'}],['$_']}]
%% After that, a bunch of data (events) is added to the table:
%% {table, {10, 20, 30}}, {table, {100, 200, 300}}, {table, {10, 10, 10}},
%% {table, {20, 20, 20}}, {table, {30, 20, 10}}
%% After that, the predicate receives this message (event):
%% {event, {20, 200, 2000}}
%% '$4' in the "match specification stub" is substituted by 20 - the first
%% item in the event's tuple (the remaining items are not used, but would
%% be substituted for '$5' and '$6' in a similar way)
%% The resulting match specification looks like this:
%% [{{'$1','$2','$3'},[{'>','$1',20}],['$_']}]
%% This is then matched against the table data (using ets:select/2)
%% and if the result is not empty, it's send to the receiver (using notify/4).
%% In this case, the result is [{30, 20, 10}, {100, 200, 300}] (see ['$_'])
%% If the received message is {event, {200, 100, 0}}, then the match spec. is
%% [{{'$1','$2','$3'},[{'>','$1',200}],['$_']}]
%% and the result is empty - []
%% so the receiver is not notified

%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = join2:start([{{'$1','$2','$3'},[{'>','$1','$4'}],['$_']}], Ref, infinity).
%% gen_server:cast(Server, {table, {10, 20, 30}}).
%% gen_server:cast(Server, {table, {100, 200, 300}}).
%% gen_server:cast(Server, {table, {10, 10, 10}}).
%% gen_server:cast(Server, {table, {20, 20, 20}}).
%% gen_server:cast(Server, {table, {30, 20, 10}}).
%% gen_server:cast(Server, {event, {20, 20, 20}}).
%% (notify is called)
%% gen_server:cast(Server, {event, {200, 100, 0}}).
%% (notify is not called)
%% ...

%% TODO - implement the notify/4 function

-module(join2).

-behaviour(gen_server).

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TABLE_NAME, events).

-record(state, {predicate_ms_stub, callback,
				benchmark_callback, benchmark_start_time, timeout, is_last_timeout}).

start(PredicateMSStub, Callback, TimeOut) ->
	gen_server:start(?MODULE, [PredicateMSStub, Callback, TimeOut], []).

start_link(PredicateMSStub, Callback, TimeOut) ->
	gen_server:start_link(?MODULE, [PredicateMSStub, Callback, TimeOut], []).

% bag - table can't contain duplicate items
init([PredicateMSStub, Callback, TimeOut]) ->
	ets:new(?TABLE_NAME, [bag, named_table, {keypos, 1}]),
	{ok, #state{predicate_ms_stub=PredicateMSStub,
				callback=Callback, timeout=TimeOut}, TimeOut}.

% set benchmark related information, store the current time
handle_call({start_benchmark, IsLast, BenchmarkCallback}, _From, State) ->
	TimeOut = State#state.timeout,
	{reply, ok, State#state{benchmark_callback=BenchmarkCallback,
							benchmark_start_time=erlang:now(),
							is_last_timeout=IsLast}, TimeOut};

handle_call(_E, _From, State) ->
    {reply, ok, State}.

% not implemented yet
notify(_Callback, _Event, _Matched, _PredicateMSStub) ->
	ok.

match(PredicateMSStub, Event = {_X, _Y, _Z}, Callback) ->
	MatchSpec = replace(PredicateMSStub, Event),
	Matched = ets:select(?TABLE_NAME, MatchSpec),
	case Matched of
		[] -> ok;
		_ -> notify(Callback, Event, Matched, PredicateMSStub)
	end.

handle_cast({table, Event}, State) ->
	ets:insert(?TABLE_NAME, Event),
	{noreply, State, State#state.timeout};

handle_cast({event, Event}, State) ->
	match(State#state.predicate_ms_stub, Event, State#state.callback),
	{noreply, State, State#state.timeout};

handle_cast(_E, State) ->
    {noreply, State}.

replace_list(Token, {X, Y, Z}) ->
	lists:map(fun(Item) -> replace(Item, {X, Y, Z}) end, Token).

replace_tuple(Token, {X, Y, Z}) ->
	erlang:list_to_tuple(replace_list(erlang:tuple_to_list(Token), {X, Y, Z})).

% replaces all (in any list or tuple) occurrences of '$4', '$5' and '$6'
% with the appropriate event values (X, Y or Z)
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