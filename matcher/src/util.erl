-module(util).

-export([extract_attr_values/2, extract_attr_values/4]).

extract_attr_values_acc([], _Event, Acc) ->
	Acc;

extract_attr_values_acc(AttrNameList, Event, Acc) ->
	[AttrName | ListTail] = AttrNameList,
	case orddict:find(AttrName, Event) of
		{ok, Val} -> extract_attr_values_acc(ListTail, Event, [Val | Acc]);
		_ -> not_found
	end.

%% Create a list with values of the attributes in the AttrNameList list
%% Return a list with the values or not_found if some attribute is not
%% contained in the event

%% filter:extract_attr_values([attr1], orddict:store(attr1, 5, orddict:store(attr2, 1, orddict:new()))). -> [5]
%% filter:extract_attr_values([attr2, attr1], orddict:store(attr1, 5, orddict:store(attr2, 1, orddict:new()))). -> [1,5]
%% filter:extract_attr_values([attr1, attr2], orddict:store(attr1, 5, orddict:store(attr2, 1, orddict:new()))). -> [5,1]
%% filter:extract_attr_values([attr1, attr2], orddict:store(attr3, 5, orddict:store(attr2, 1, orddict:new()))). -> not_found
extract_attr_values(AttrNameList, Event) ->
	case extract_attr_values_acc(AttrNameList, Event, []) of
		not_found -> not_found;
		EventValues -> lists:reverse(EventValues)
	end.

extract_attr_values(LeftAttrNameList, RightAttrNameList, LeftEvent, RightEvent) ->
	% note: if the events always have the supplied attributes, it will be faster
	% if the result is tested (if it's equal to not_found) only once - at the end
	LeftEventValues = extract_attr_values_acc(LeftAttrNameList, LeftEvent, []),
	case LeftEventValues of
		not_found -> not_found;
		_ ->
			EventValues = extract_attr_values_acc(RightAttrNameList,
				RightEvent, LeftEventValues),
			case EventValues of
				not_found -> not_found;
				_ -> lists:reverse(EventValues)
			end
	end.