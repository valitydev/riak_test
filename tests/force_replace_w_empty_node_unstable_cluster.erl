%% -------------------------------------------------------------------
%%% @copyright (C) 2020, rbk.money
%%% @doc
%%% riak_test for clean force replace of a faulty node in unstalbe
%%% cluster
%%%
%%% Scenario, roughly:
%%%
%%% 1. Set up a cluster of several (5 by default) nodes of 'previous'
%%% version (i.e. 2.2.3).
%%%    Designate some node (middle one, `dev3` by default) as a 'leaving'
%%%    node.
%%%
%%% 2. Wait for ring to converge and initial handoffs to complete.
%%%
%%% 3. Start a set of workers writing and reading keys concurrently and
%%% warm up (for 20 minutes by default) a cluster.
%%%    Two workers per 4 out of 5 nodes by default, 'leaving' node is
%%%    one without workers. Each worker is a pair of load generator, who is
%%%    responsible for writing keys sequentially one after another, with
%%%    some fixed key prefix, and verifier, responsible for issuing reads
%%%    for keyspace covered so far and keeping score on how these reads fail
%%%    and how often.
%%%
%%% 4. Join one more node to a cluster, this time of 'current' version
%%% (i.e. 2.9.2) and with limited handoff concurrency.
%%%    Designate this node as 'faulty'.
%%%
%%% 5. Wait for ownership transfers to kick off, then pick one at random and
%%% make sure it never completes.
%%%    This is accomplished with an intercept which abnormally terminates
%%%    handoff receiver process right before completion, upon receiving 'sync'
%%%    message from the sender.
%%%
%%% 6. Wait for (at least half by default) ownership transfers to this 'faulty'
%%% node to complete.
%%%
%%% 7. Wait for 'leaving' node to finish all ownership transfers to the
%%% 'faulty' node.
%%%
%%% 8. Order 'leaving' node, well, to leave the cluster finally.
%%%
%%% 8. Give cluster some time (3 minutes by default) to stabilize.
%%%
%%% 9. Kill 'faulty' node, brutally, without a chance to say farewell to the
%%% cluster.
%%%
%%% 10. Mark 'faulty' node as 'down' by hand.
%%%
%%% 11. Wait for 'leaving' node to hand ownership off of all their partitions.
%%%
%%% 12. Join and force-replace 'faulty' with one more node of 'current'
%%% version.
%%%
%%% 13. Ensure that this new node will eventually take ownership of those
%%% partitions previously owned by 'faulty'.
%%%    To be honest we do not check that ownership is strictly the same, just
%%%    waiting for several handoffs to this node to complete.
%%%
%%% 14. Stop the workers.
%%%    Ideally, we want workers to report no errors at all at the end of a test
%%%    run. Few errors signifying partial unavailability are ok though.
%%%
%%% @end

-module(force_replace_w_empty_node_unstable_cluster).
-behavior(riak_test).

-compile({parse_transform, rt_intercept_pt}).
-define(M, riak_core_handoff_receiver_orig).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).

% -record(handoff_receiver, {
%     sock :: port(),
%     peer :: term(),
%     ssl_opts :: [] | list(),
%     tcp_mod :: atom(),
%     recv_timeout_len :: non_neg_integer(),
%     vnode_timeout_len :: non_neg_integer(),
%     partition :: non_neg_integer(),
-define(HANDOFF_RECEIVER_STATE_PARTITION, 8).
%     vnode_mod = riak_kv_vnode:: module(),
%     vnode :: pid(),
%     count = 0 :: non_neg_integer()
% }).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"buck">>).
-define(HANDOFF_CONCURRENCY, 8).
-define(DEFAULT_BUCKET_PROPS, [
    {n_val, 3},
    {allow_mult, false},
    {dvv_enabled, false},
    {last_write_wins, false}
]).

-define(CLIENT_CONCURRENCY, 2).
-define(GET_TIMEOUT, 3000).
-define(PUT_TIMEOUT, 3000).
-define(GET_OPTS, [
    {r, quorum},
    {pr, quorum}
]).
-define(PUT_OPTS, [
    {w, quorum},
    {pw, quorum},
    {dw, quorum}
]).

-define(RIAK_223_NODES, 5).

-define(CLUSTER_WARMUP_TIME, 20 * 60 * 1000).
-define(FLAPPY_HANDOFF_COMPLETE_PERCENT, 50).
-define(FLAPPY_HANDOFF_COMPLETE_TIME, 10 * 60 * 1000).
-define(LEAVING_FREE_TIME, 3 * 60 * 1000).
-define(FLAPPY_FREE_TIME, 3 * 60 * 1000).
-define(REPLACE_FREE_TIME, 3 * 60 * 1000).
-define(LEAVING_HANDOFF_COMPLETE_TIME, 10 * 60 * 1000).

-define(HANDOFF_START_DELAY, 12000).
-define(HANDOFF_PROBE_DELAY, 10000).

confirm() ->
    Conf = [
        {riak_core, [
            {default_bucket_props, ?DEFAULT_BUCKET_PROPS},
            {ring_creation_size, 64},
            {handoff_concurrency, ?HANDOFF_CONCURRENCY}
        ]}
    ],
    ConfFlappy = [
        {riak_core, [
            {default_bucket_props, ?DEFAULT_BUCKET_PROPS},
            {ring_creation_size, 64},
            {handoff_concurrency, 2}
        ]}
    ],

    ClusterSetup =
        lists:duplicate(?RIAK_223_NODES, {previous, Conf}) ++ [
            {current, ConfFlappy},
            {current, Conf}
        ],
    NodesAll = rt:deploy_nodes(ClusterSetup),
    {Nodes = [Node1 | _], NodesExtra = [NodeFlappy, NodeReserve]} =
        lists:split(?RIAK_223_NODES, NodesAll),

    ok = rt:join_cluster(Nodes),
    lager:info("Initial cluster built: ~p", [Nodes]),
    lager:info("Extra nodes not in cluster yet: ~p", [NodesExtra]),

    ok = timer:sleep(?HANDOFF_START_DELAY),
    ok = rt:wait_until_transfers_complete(Nodes),
    ok = lists:foreach(fun rt:wait_until_node_handoffs_complete/1, Nodes),

    NodeLeaving = lists:nth(round(?RIAK_223_NODES / 2), Nodes),
    Clients = [rt:pbc(N) ||
        Ns <- lists:duplicate(?CLIENT_CONCURRENCY, Nodes),
        N <- Ns,
            N /= NodeLeaving
    ],

    lager:info("Starting ~p workers...", [length(Clients)]),
    Workers = start_workers(Clients),

    lager:info("Warming cluster up for ~ps ...", [?CLUSTER_WARMUP_TIME div 1000]),
    ok = timer:sleep(?CLUSTER_WARMUP_TIME),

    lager:info("Leaving node = ~p", [NodeLeaving]),
    lager:info("Flappy node = ~p", [NodeFlappy]),
    lager:info("Reserve node = ~p", [NodeReserve]),

    lager:info("Joining node ~p in...", [NodeFlappy]),
    _ = rt:staged_join(NodeFlappy, Node1),
    ok = rt:plan_and_commit(Node1),
    ok = rt:try_nodes_ready(Nodes),


    ok = timer:sleep(?HANDOFF_START_DELAY),
    Handoffs = determine_planned_handoffs_to(NodeFlappy, Nodes -- [NodeLeaving]),
    Handoff = {_Owner, Idx} = lists:nth(random:uniform(length(Handoffs)), Handoffs),
    lager:info("Ensure ~p never completes handoff of ~p...", [NodeFlappy, Handoff]),
    rt_intercept:load_code(NodeFlappy),
    rt_intercept:add(NodeFlappy,
        {riak_core_handoff_receiver, [
            {{process_message, 3}, {
                [Idx],
                fun
                    (Ty, Data, St) when Ty == ?PT_MSG_SYNC; Ty == ?PT_MSG_OLDSYNC ->
                        case erlang:element(?HANDOFF_RECEIVER_STATE_PARTITION, St) of
                            Idx -> error({aborted, ?MODULE});
                            _   -> ?M:process_message_orig(Ty, Data, St)
                        end;
                    (Ty, Data, St) ->
                        ?M:process_message_orig(Ty, Data, St)
                end
            }}
        ]}
    ),

    ok = wait_handoffs_complete_to(NodeFlappy, Nodes, ?FLAPPY_HANDOFF_COMPLETE_PERCENT / 100),
    ok = wait_handoffs_complete_to(NodeFlappy, [NodeLeaving], 1.0),
    _ = determine_node_partitions(NodeFlappy),

    ok = rt:leave(NodeLeaving),
    ok = rt:wait_until_nodes_ready(Nodes),

    lager:info("Allow node to leave for ~ps ...", [?LEAVING_FREE_TIME div 1000]),
    ok = timer:sleep(?LEAVING_FREE_TIME),

    ok = kill_node(NodeFlappy),
    ok = timer:sleep(?HANDOFF_START_DELAY),
    lager:info("Mark ~p as down, so ~p could finally leave ...", [NodeFlappy, NodeLeaving]),
    ok = rt:down(NodeLeaving, NodeFlappy),

    ok = timer:sleep(?HANDOFF_START_DELAY),
    ok = wait_handoffs_complete_from(NodeLeaving),
    ok = wait_no_primary_left(NodeLeaving),

    lager:info("Observing flappy node for ~ps more ...", [?FLAPPY_FREE_TIME div 1000]),
    ok = timer:sleep(?FLAPPY_FREE_TIME),

    lager:info("Force replacing ~p with ~p ...", [NodeFlappy, NodeReserve]),
    ok = rt:staged_join(NodeReserve, Node1),
    ok = plan(Node1),
    ok = staged_force_replace(Node1, NodeFlappy, NodeReserve),
    ok = plan_and_commit(Node1),
    ok = rt:try_nodes_ready([NodeReserve | Nodes]),
    ok = timer:sleep(?HANDOFF_START_DELAY),
    ok = wait_handoffs_complete_to(NodeReserve, Nodes -- [NodeLeaving], 1.0),

    lager:info("Observing cluster for ~ps more ...", [?REPLACE_FREE_TIME div 1000]),
    ok = timer:sleep(?REPLACE_FREE_TIME),

    _ = stop_workers(Workers),

    pass.

wait_handoffs_complete_to(Node, Cluster, Fraction) when Fraction > 0.0, Fraction =< 1.0 ->
    lager:info("Wait for at least ~p% of ownership handoffs from ~p to ~p to complete...",
        [round(Fraction * 100), Cluster, Node]),
    Total = length(determine_planned_handoffs_to(Node, Cluster)),
    Target = round((1.0 - Fraction) * Total),
    if
        Total == Target ->
            ok;
        true ->
            rt:wait_until(
                fun () -> determine_num_planned_handoffs_to(Node, Cluster) =< Target end,
                ?FLAPPY_HANDOFF_COMPLETE_TIME div ?HANDOFF_PROBE_DELAY,
                ?HANDOFF_PROBE_DELAY
            )
    end.

determine_num_planned_handoffs_to(Node, Cluster) ->
    Changes = determine_planned_handoffs_to(Node, Cluster),
    lager:info("~p outstanding handoffs to ~p: ~p", [length(Changes), Node, Changes]),
    length(Changes).

wait_handoffs_complete_from(Node) ->
    lager:info("Wait for ownership handoffs from ~p to complete...", [Node]),
    rt:wait_until(
        fun () -> determine_planned_handoffs_from(Node) == 0 end,
        ?LEAVING_HANDOFF_COMPLETE_TIME div ?HANDOFF_PROBE_DELAY,
        ?HANDOFF_PROBE_DELAY
    ).

wait_no_primary_left(Node) ->
    ok = rt:wait_until(
        fun () ->
            {Primary, _, _} = determine_node_partitions(Node),
            length(Primary) == 0
        end,
        ?LEAVING_HANDOFF_COMPLETE_TIME div ?HANDOFF_PROBE_DELAY,
        ?HANDOFF_PROBE_DELAY
    ).

determine_planned_handoffs_to(Node, Cluster) ->
    {Rings, []} = rpc:multicall(Cluster, riak_core_ring_manager, get_raw_ring, []),
    lists:usort([{Owner, Idx} ||
        {ok, Ring} <- Rings,
        {Idx, Owner, NextOwner, _Mod, Status} <- riak_core_ring:pending_changes(Ring),
            Status == awaiting,
            NextOwner == Node,
            lists:member(Owner, Cluster)
    ]).

determine_planned_handoffs_from(Node) ->
    Ring = rt:get_ring(Node),
    Changes = lists:usort([{NextOwner, Idx} ||
        {Idx, Owner, NextOwner, _Mod, Status} <- riak_core_ring:pending_changes(Ring),
            Owner == Node,
            Status == awaiting
    ]),
    lager:info("~p outstanding handoffs from ~p: ~p", [length(Changes), Node, Changes]),
    length(Changes).

determine_node_partitions(Node) ->
    Ring = rt:get_ring(Node),
    Partitions = {Primary, Secondary, Stopped} = riak_core_status:partitions(Node, Ring),
    lager:info("Partitions for ~p: ~p primary, ~p secondary, ~p stopped",
        [Node, length(Primary), length(Secondary), length(Stopped)]),
    Partitions.

%%

staged_force_replace(Node, NodeToReplace, NodeWith) ->
    rpc:call(Node, riak_core_claimant, force_replace, [NodeToReplace, NodeWith]).

plan(Node) ->
    timer:sleep(500),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            lager:info("plan: ring not ready"),
            plan(Node);
        {ok, Actions, _Transitions} ->
            lager:info("plan: ok"),
            lager:info("actions = ~p", [Actions]),
            % lager:info("transitions = ~p", [Transitions]),
            ok
    end.

plan_and_commit(Node) ->
    timer:sleep(500),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            lager:info("plan: ring not ready"),
            plan_and_commit(Node);
        {ok, Actions, _Transitions} ->
            lager:info("plan: ok"),
            lager:info("actions = ~p", [Actions]),
            % lager:info("transitions = ~p", [Transitions]),
            rt:do_commit(Node)
    end.

%%

kill_node(Node) ->
    lager:info("Killing node ~p ...", [Node]),
    OSPid = rpc:call(Node, os, getpid, []),
    _ = rpc:cast(Node, os, cmd, [io_lib:format("kill -9 ~s", [OSPid])]),
    ok.

%%

start_workers(Clients) ->
    [start_worker(N, Client) ||
        {N, Client} <- lists:zip(lists:seq(1, length(Clients)), Clients)].

start_worker(N, Client) ->
    KeyPrefix = integer_to_binary(N),
    Verifier = spawn_link(fun () -> verifier(KeyPrefix, 1, Client) end),
    LoadGen = spawn_link(fun () -> load_generator(KeyPrefix, 1, Client, Verifier) end),
    {Verifier, LoadGen}.

stop_workers(Workers) ->
    lists:foreach(
        fun ({Verifier, LoadGen}) ->
            ok = stop_one(Verifier),
            ok = stop_one(LoadGen)
        end,
        Workers
    ).

stop_one(Pid) ->
    stop_one(Pid, 30000).

stop_one(Pid, Timeout) ->
    Pid ! {stop, self()},
    receive {stopped, Pid} -> ok after Timeout -> error(timeout) end.

%%

-define(REPORT_STAT_PROB, 0.0005).

-record(loadgen, {
    client,
    key_prefix,
    verifier,
    counter = 1,
    value_size = {400, 800},
    sleep_interval = {12, 20},
    report_progress_prob = 0.05,
    errors = orddict:new()
}).

load_generator(KeyPrefix, Start, Client, Verifier) ->
    _ = random:seed(erlang:now()),
    load_generator(#loadgen{
        client = Client,
        key_prefix = KeyPrefix,
        verifier = Verifier,
        counter = Start
    }).

load_generator(St = #loadgen{}) ->
    _ = probably(?REPORT_STAT_PROB,
        fun () -> lager:info("~p", [lager:pr(St, ?MODULE)]) end
    ),
    St1 = #loadgen{counter = C1} = put_object(St),
    ok = timer:sleep(rand_in_range(St1#loadgen.sleep_interval)),
    _ = probably(St#loadgen.report_progress_prob,
        fun () -> St#loadgen.verifier ! {last, C1} end
    ),
    receive
        {report, Mark} ->
            lager:info("~s: ~p", [Mark, lager:pr(St, ?MODULE)]),
            load_generator(St1);
        {stop, From} ->
            lager:info("finished: ~p", [lager:pr(St, ?MODULE)]),
            From ! {stopped, self()}
    after 0 ->
        load_generator(St1)
    end.

put_object(St = #loadgen{counter = C, errors = Errors}) ->
    Ts = os:timestamp(),
    Res = put_object(
        St#loadgen.client,
        mk_key(St#loadgen.key_prefix, C),
        mk_value(rand_in_range(St#loadgen.value_size)),
        mk_index(Ts)),
    case Res of
        ok ->
            St#loadgen{counter = C + 1};
        {error, Reason} ->
            St#loadgen{errors = orddict:update_counter(Reason, 1, Errors)}
    end.

%%

-record(verifier, {
    client,
    key_prefix,
    first,
    last,
    sleep_interval = {8, 15},
    skew = 0.35,
    done = 0,
    errors = orddict:new()
}).

verifier(KeyPrefix, Start, Client) ->
    _ = random:seed(erlang:now()),
    verifier(#verifier{
        client = Client,
        key_prefix = KeyPrefix,
        first = Start
    }).

verifier(St = #verifier{last = undefined}) ->
    receive
        {last, C1} ->
            verifier(St#verifier{last = C1});
        {stop, From} ->
            lager:info("finished: ~p", [lager:pr(St, ?MODULE)]),
            From ! {stopped, self()}
    end;
verifier(St = #verifier{}) ->
    _ = probably(?REPORT_STAT_PROB,
        fun () -> lager:info("~p", [lager:pr(St, ?MODULE)]) end
    ),
    receive
        {last, C1} ->
            verifier(St#verifier{last = C1});
        {report, Mark} ->
            lager:info("~s: ~p", [Mark, lager:pr(St, ?MODULE)]),
            verifier(St);
        {stop, From} ->
            lager:info("finished: ~p", [lager:pr(St, ?MODULE)]),
            From ! {stopped, self()}
    after 0 ->
        St1 = probe(St),
        ok = timer:sleep(rand_in_range(St1#verifier.sleep_interval)),
        verifier(St1)
    end.

probe(St = #verifier{key_prefix = Prefix, first = F, last = L, done = D, errors = Errors}) ->
    % skew towards recent writes
    R = math:pow(random:uniform(), St#verifier.skew),
    K = trunc((L - F) * R) + F,
    Res = riakc_pb_socket:get(St#verifier.client, ?BUCKET, mk_key(Prefix, K), ?GET_OPTS, ?GET_TIMEOUT),
    case Res of
        {ok, _} ->
            St#verifier{done = D + 1};
        {error, Reason} ->
            St#verifier{errors = orddict:update_counter(Reason, 1, Errors)}
    end.

%%

%%

mk_key(Prefix, C) ->
    <<Prefix/binary, ":", (integer_to_binary(C))/binary>>.

mk_value(ValueSize) ->
    mk_value(ValueSize, <<>>).

mk_value(N, Acc) when N > 0 ->
    R32 = random:uniform(16#FFFFFFFF),
    mk_value(N - 4, <<Acc/binary, R32:32/integer>>);
mk_value(_, Acc) ->
    Acc.

mk_index({MSecs, Secs, USecs}) ->
    ((MSecs * 1000000) + Secs) * 1000 + USecs div 1000.

put_object(Pid, Key, Value, Index) when is_integer(Index) ->
    put_object(Pid, Key, Value, [{"timestamp_int", Index}]);

put_object(Pid, Key, Data, Indexes) when is_list(Indexes) ->
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2, ?PUT_OPTS, ?PUT_TIMEOUT).

%%

rand_in_range({Min, Max}) ->
    Min + random:uniform(Max - Min + 1) - 1.

probably(P0, Fun) ->
    case random:uniform() of
        P when P < P0 -> Fun();
        _ -> undefined
    end.
