%%
%% Copyright (C) 2013-2013 UENISHI Kota
%%
%%    Licensed under the Apache License, Version 2.0 (the "License");
%%    you may not use this file except in compliance with the License.
%%    You may obtain a copy of the License at
%%
%%        http://www.apache.org/licenses/LICENSE-2.0
%%
%%    Unless required by applicable law or agreed to in writing, software
%%    distributed under the License is distributed on an "AS IS" BASIS,
%%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%    See the License for the specific language governing permissions and
%%    limitations under the License.
%%
%% @doc
%%   Simple Generic Queue - a wrapper of queue()
%%   This module is named gen_ but no behaviour to implement.
%% @end

-module(gen_queue).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1,
         push/1,
         pop/0,
         push/2,
         pop/1,
         %% nb_push/2,
         %% nb_pop/1
         info/0,
         info/1,
         stop/1
        ]).

-include_lib("eunit/include/eunit.hrl").
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          dataq = queue:new() :: queue(),
          waitq = queue:new() :: queue()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
start_link() ->
    start_link(?SERVER).
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%% if we make `push` a blocking call, set it as infinity
push(Pid, Thingie) -> gen_server:call(Pid, {push, Thingie}).
pop(Pid) -> gen_server:call(Pid, pop, infinity).
push(Thingie) -> push(?SERVER, Thingie).
pop() -> pop(?SERVER).

%% nb_push(Pid, Thingie) -> gen_server:call(Pid, {nb_push, Thingie}).
%% nb_pop(Pid) -> gen_server:call(Pid, nb_pop).

info() -> info(?SERVER).
info(Pid) -> gen_server:call(Pid, info).

stop(Pid) -> gen_server:call(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
init([]) ->
    {ok, #state{}}.

%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
handle_call({push, Thingie}, _, #state{dataq=Q,waitq=WQ} = State) ->
    case {queue:is_empty(Q), queue:is_empty(WQ)} of
        {_, true} -> %% none waiting
            Q2 = queue:in(Thingie, Q),
            {reply, ok, State#state{dataq=Q2}};
        {true, false} -> %% starving workers
            {{value, Worker}, WQ2} = queue:out(WQ),
            gen_server:reply(Worker, {ok, Thingie}),
            {reply, ok, State#state{waitq=WQ2}};
        {false, false} -> %% something wrong but try to make it sane
            Q2 = queue:in(Thingie, Q),
            {reply, ok, handle_both_not_empty(State#state{dataq=Q2})}
    end;

handle_call(pop, From, #state{dataq=Q,waitq=WQ} = State) ->
    case {queue:is_empty(Q), queue:is_empty(WQ)} of
        {false, true} -> %% none waiting
            {{value, Item}, Q2} = queue:out(Q),
            {reply, {ok, Item}, State#state{dataq=Q2}};
        {true, _} -> %% starving workers
            WQ2 = queue:in(From, WQ),
            {noreply, State#state{waitq=WQ2}};
        {false, false} -> %% something wrong but try to make it sane
            WQ2 = queue:in(From, WQ),
            {noreply, handle_both_not_empty(State#state{waitq=WQ2})}
    end;

handle_call(info, _, #state{dataq=Q,waitq=WQ} = State) ->
    {reply, [{length,queue:len(Q)}, {waiting,queue:len(WQ)}], State};

handle_call(stop, _, State) ->
    {stop, normal, ok, State}.

handle_both_not_empty(#state{dataq=Q,waitq=WQ} = State) ->
    DataLen = queue:len(Q),
    WaitLen = queue:len(WQ),
    case {DataLen, WaitLen} of
        _ when DataLen >= WaitLen ->
            {Data0, Rest} = lists:split(WaitLen, queue:to_list(Q)),
            Wait0 = queue:to_list(WQ),
            [gen_server:reply(Waiter, {ok, Data}) || {Data, Waiter} <- lists:zip(Data0, Wait0)],
            State#state{dataq=queue:from_list(Rest),waitq=queue:new()};
        _ when DataLen < WaitLen ->
            Data0 = queue:to_list(Q),
            {Wait0, Rest} = lists:split(DataLen, queue:to_list(WQ)),
            [gen_server:reply(Waiter, {ok, Data}) || {Data, Waiter} <- lists:zip(Data0, Wait0)],
            State#state{dataq=queue:new(),waitq=queue:from_list(Rest)}
    end.

%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{waitq=WQ} = _State) ->
    [gen_server:reply(Waiter, {error, {terminated, _Reason}})
     || Waiter <- queue:to_list(WQ)],
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



-ifdef(TEST).

gen_queue_test() ->
    {ok, Pid} = gen_queue:start_link(),
    ?assert(is_pid(Pid)),
    ok = gen_queue:push(a),
    {ok, a} = gen_queue:pop(),
    ok = gen_queue:stop(Pid).

named_test() ->
    {ok, Pid} = gen_queue:start_link(name),
    ?assert(is_pid(Pid)),
    ok = gen_queue:push(name, a),
    {ok, a} = gen_queue:pop(name),
    ok = gen_queue:stop(name).

-endif.
