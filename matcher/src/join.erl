%% A join predicate

%% Input arguments:
%%
%% Predicate - a simple fun(ction) that returns true or false and can be
%% converted to match specification
%% For example: "fun({A, B}) -> A - B > 1000 end"
%%
%% LeftAttrNameList - list of "left" event attributes corresponding to the
%% fun arguments. For example: [total_users]
%% RightAttrNameList - list of "right" event attributes corresponding to the
%% fun arguments. For example: [active_users]
%% (The attributes in LeftAttrNameList substitute the first (left) attributes
%% of the predicate function, the rest of the attributes is substituted by the
%% attributes in the RightAttrNameList)
%%
%% If predicate equals "fun({A, B}) -> A - B > 1000 end", LeftAttrNameList
%% equals [total_users] and RightAttrNameList equals [active_users], then a
%% "left" event matches the predicate (causing the notify/4 function to be
%% called) if the total_users attribute value in this event minus the
%% active_users attribute value in the "right" event at the top of the queue
%% for right events is greater than 1000. Similarly for a "right" event.
%% After match test, both events are discarded (it doesn't matter what the
%% match result is). The implication is at least one of the two queues is
%% empty at any given time.
%%
%% Callback - An object that will get a notification
%% if an incoming event matches the predicate

%% Events are represented as orddicts
%% Usage example (replace Ref with the actual receiver reference):
%% {ok, Server} = join:start(fun({A, B}) -> A - B > 1000 end, [total_users], [active_users], Ref).
%% gen_server:cast(Server, {left, orddict:store(total_users, 10000, orddict:new())}).
%% gen_server:cast(Server, {right, orddict:store(active_users, 5000, orddict:new())}).
%% (notify is called)
%% ...

%% TODO - implement the notify/4 function

-module(join).

-behaviour(gen_server).

%% API
-export([start/4, start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {predicate, left_attr_list, right_attr_list,
	left_queue, right_queue, callback}).

start(Predicate, LeftAttrNameList, RightAttrNameList, Callback) ->
	gen_server:start(?MODULE, [Predicate, LeftAttrNameList, RightAttrNameList, Callback], []).

start_link(Predicate, LeftAttrNameList, RightAttrNameList, Callback) ->
	gen_server:start_link(?MODULE, [Predicate, LeftAttrNameList, RightAttrNameList, Callback], []).

init([Predicate, LeftAttrNameList, RightAttrNameList, Callback]) ->
	MatchSpec = ets:fun2ms(Predicate),
	case MatchSpec of
		{error, Error} -> {stop, Error};
		_ ->
			% check the AttrNameList has the correct number of parameters
			case tuple_size(element(1, hd(MatchSpec))) ==
				(length(LeftAttrNameList)) + (length(RightAttrNameList)) of
				true -> {ok, #state{predicate=ets:match_spec_compile(MatchSpec),
						left_attr_list=LeftAttrNameList,
						right_attr_list=RightAttrNameList,
						left_queue=queue:new(), right_queue=queue:new(),
						callback=Callback}};
				_ -> {stop, wrong_number_of_parameters}
			end
	end.

handle_call(_E, _From, State) ->
    {noreply, State}.

% not implemented yet
notify(_Callback, _LeftEvent, _RightEvent, _Predicate) ->
	ok.

%% check if the two events match the predicate
%% return true if they do, false otherwise
match(Predicate, LeftEvent, RightEvent, LeftAttrList, RightAttrList, Callback) ->
	OrderedAttrValList = util:extract_attr_values(LeftAttrList, RightAttrList,
		LeftEvent, RightEvent),
	case OrderedAttrValList of
		not_found -> false;
		_ -> case ets:match_spec_run([list_to_tuple(OrderedAttrValList)], Predicate) of
			[true] -> notify(Callback, LeftEvent, RightEvent, Predicate), true;
			_ -> false
		end
	end.

%% handle "left" event
handle_cast({left, LeftEvent}, State=#state{left_attr_list=LeftAttrList,
	right_attr_list=RightAttrList, left_queue=LeftQueue, right_queue=RightQueue,
	predicate=Predicate, callback=Callback}) ->
	NewState = case queue:out(RightQueue) of
		{empty, _} -> State#state{left_queue=queue:in(LeftEvent, LeftQueue)};
		{{value, RightEvent}, NewRightQueue} ->
			match(Predicate, LeftEvent, RightEvent,
				  LeftAttrList, RightAttrList, Callback),
			State#state{right_queue=NewRightQueue}
	end,
	{noreply, NewState};

%% handle "right" event
%% (code is duplicated, but there are no redundant comparisons)
handle_cast({right, RightEvent}, State=#state{left_attr_list=LeftAttrList,
	right_attr_list=RightAttrList, left_queue=LeftQueue, right_queue=RightQueue,
	predicate=Predicate, callback=Callback}) ->
	NewState = case queue:out(LeftQueue) of
		{empty, _} -> State#state{right_queue=queue:in(RightEvent, RightQueue)};
		{{value, LeftEvent}, NewLeftQueue} ->
			match(Predicate, LeftEvent, RightEvent,
				  LeftAttrList, RightAttrList, Callback),
			State#state{left_queue=NewLeftQueue}
	end,
	{noreply, NewState};

handle_cast(_E, State) ->
    {noreply, State}.

handle_info(_E, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.