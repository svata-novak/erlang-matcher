%% A filter predicate

%% Input arguments:
%%
%% Predicate - a simple fun(ction) that returns true or false and can be
%% converted to match specification
%% For example: "fun({A}) -> A > 1000 end" or
%% "fun({A, B}) when is_integer(A) -> abs(A) + abs(B) > 1000 end"
%%
%% AttrNameList - list of event attributes corresponding to the fun arguments
%% For example: [size] or [x, y]
%%
%% If predicate equals "fun({A}) -> A > 1000 end" and AttrNameList equals [size]
%% then an event matches the predicate (causing the notify/3 function to be
%% called) if the event has an attribute called size which has a value greater
%% than 1000
%%
%% Callback - An object that will get a notification
%% if an incoming event matches the predicate

%% Events are represented as orddicts
%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = filter:start(fun({A}) -> A > 1000 end, [size], Ref).
%% gen_server:cast(Server, {event, orddict:store(size, 500, orddict:new())}).
%% (notify is not called)
%% gen_server:cast(Server, {event, orddict:store(size, 2000, orddict:new())}).
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

-record(state, {predicate, attr_list, callback, event_count, event_notification_count, benchmark_callback}).

start(Predicate, AttrNameList, Callback) ->
	gen_server:start(?MODULE, [Predicate, AttrNameList, Callback], []).

start_link(Predicate, AttrNameList, Callback) ->
	gen_server:start_link(?MODULE, [Predicate, AttrNameList, Callback], []).

init([Predicate, AttrNameList, Callback]) ->
	MatchSpec = ets:fun2ms(Predicate),
	case MatchSpec of
		{error, Error} -> {stop, Error};
		_ ->
			% check the AttrNameList has the correct number of parameters
			case tuple_size(element(1, hd(MatchSpec))) == length(AttrNameList) of
				true -> {ok, #state{predicate=ets:match_spec_compile(MatchSpec),
						 attr_list=AttrNameList, callback=Callback, event_count=0,
						 event_notification_count=-1}};
				_ -> {stop, wrong_number_of_parameters}
			end
	end.

handle_call({set_benchmark_data, {BenchmarkCallback, EventNotificationCount}}, _From, State) ->
	{reply, ok, State#state{benchmark_callback=BenchmarkCallback,
							event_notification_count=EventNotificationCount}};

handle_call(get_event_count, _From, State=#state{event_count=EventCount}) ->
	{reply, EventCount, State};

handle_call(_E, _From, State) ->
    {noreply, State}.

% not implemented yet
notify(_Callback, _Event, _Predicate) ->
	ok.

update_event_count(State=#state{event_count=EventCount,
								event_notification_count=EventNotificationCount,
								benchmark_callback=BenchmarkCallback}) ->
	NewEventCount = EventCount + 1,
	case NewEventCount of
		EventNotificationCount -> BenchmarkCallback ! EventNotificationCount;
		_ -> ok
	end,
		
	State#state{event_count=NewEventCount}.

%% handle incoming events, notify the observer if the event matches the predicate
handle_cast({event, Event}, State=#state{attr_list=AttrList,
	predicate=Predicate, callback=Callback}) ->
	OrderedAttrValList = util:extract_attr_values(AttrList, Event),
	case OrderedAttrValList of
		not_found -> ok; % some predicate attribute is not contained in the event
		_ -> case ets:match_spec_run([list_to_tuple(OrderedAttrValList)], Predicate) of
			[true] -> notify(Callback, Event, Predicate); % matched
			_ -> ok % didn't match
		end
	end,
	{noreply, update_event_count(State)};

handle_cast(_E, State) ->
    {noreply, State}.

handle_info(_E, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
