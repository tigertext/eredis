-module(eredis_sub_ssl_client_tests).

-include_lib("eunit/include/eunit.hrl").
-include("eredis.hrl").
-include("eredis_sub.hrl").

state() ->
    #state{socket = make_ref(),
           controlling_process = {make_ref(), self()},
           msg_queue = queue:new(),
           max_queue_size = 40000,
           queue_behaviour = exit,
           msg_state = need_ack}.

msg() -> {message, <<"c">>, <<"m">>, self()}.

enqueue(N, S) ->
    lists:foldl(fun(_, Acc) -> eredis_sub_ssl_client:queue_or_send(msg(), Acc) end,
                S, lists:seq(1, N)).

ack(S) ->
    {noreply, S1} = eredis_sub_ssl_client:handle_cast({ack_message, self()}, S),
    S1.

flush() ->
    receive _ -> flush() after 0 -> ok end.

queue_len_counter_tracks_queue_test() ->
    S = enqueue(3, state()),
    ?assertEqual(3, S#state.msg_queue_len),
    ?assertEqual(3, queue:len(S#state.msg_queue)),
    S1 = ack(S),
    ?assertEqual(2, S1#state.msg_queue_len),
    ?assertEqual(2, queue:len(S1#state.msg_queue)),
    flush().

peak_tracks_max_test() ->
    S = ack(ack(enqueue(3, state()))),
    ?assertEqual(1, S#state.msg_queue_len),
    ?assertEqual(3, S#state.peak_queue_len),
    flush().

overflow_exit_test() ->
    S = (state())#state{max_queue_size = 2, queue_behaviour = exit, msg_queue_len = 3},
    ?assertMatch({stop, max_queue_size, _}, eredis_sub_ssl_client:check_queue(S)),
    receive {exited, 3} -> ok after 0 -> ?assert(false) end.

timeout_keeps_start_limit_test() ->
    meck:new(tt_prometheus, [non_strict]),
    meck:expect(tt_prometheus, report_eredis_sub_ssl_client_state_msg_queue_len, fun(_) -> ok end),
    try
        S = (ack(enqueue(3, state())))#state{max_queue_size = 200000, tref = undefined},
        {noreply, S1} = eredis_sub_ssl_client:handle_info(timeout, S),
        ?assertEqual(200000, S1#state.max_queue_size),
        ?assertEqual(exit, S1#state.queue_behaviour),
        ?assert(meck:called(tt_prometheus, report_eredis_sub_ssl_client_state_msg_queue_len, [3])),
        ?assertEqual(2, S1#state.peak_queue_len),
        _ = timer:cancel(S1#state.tref)
    after
        meck:unload(tt_prometheus),
        flush()
    end.

start_link3_defaults_test() ->
    meck:new(eredis_sub_ssl_client, [passthrough]),
    meck:expect(eredis_sub_ssl_client, start_link, fun(_, _, _, _, M, B) -> {ok, {M, B}} end),
    meck:expect(eredis_sub_ssl_client, start_link, fun(_, _, _, _, M, B, _) -> {ok, {M, B}} end),
    try
        ?assertEqual({ok, {40000, exit}}, eredis_sub_ssl:start_link("h", 1, "")),
        ?assertEqual({ok, {40000, exit}}, eredis_sub_ssl:start_link([]))
    after
        meck:unload(eredis_sub_ssl_client)
    end.

overflow_drop_test() ->
    S = (state())#state{max_queue_size = 2, queue_behaviour = drop},
    {noreply, S1} = eredis_sub_ssl_client:check_queue(enqueue(3, S)),
    ?assertEqual(0, S1#state.msg_queue_len),
    ?assert(queue:is_empty(S1#state.msg_queue)),
    receive {dropped, 3} -> ok after 0 -> ?assert(false) end.

batch_on_ack_test() ->
    Msgs = [{message, <<"c">>, integer_to_binary(I), self()} || I <- lists:seq(1, 5)],
    S0 = (state())#state{batch_size = 3},
    S = lists:foldl(fun eredis_sub_ssl_client:queue_or_send/2, S0, Msgs),
    S1 = ack(S),
    [M1, M2, M3 | _] = Msgs,
    receive {messages, Batch, _} -> ?assertEqual([M1, M2, M3], Batch)
    after 0 -> ?assert(false) end,
    ?assertEqual(2, S1#state.msg_queue_len),
    ?assertEqual(2, queue:len(S1#state.msg_queue)),
    ?assertEqual(need_ack, S1#state.msg_state),
    flush().

ready_sends_batch_of_one_test() ->
    M = msg(),
    S = (state())#state{batch_size = 3, msg_state = ready},
    S1 = eredis_sub_ssl_client:queue_or_send(M, S),
    receive {messages, [M], _} -> ok after 0 -> ?assert(false) end,
    ?assertEqual(need_ack, S1#state.msg_state),
    flush().

batch_size_one_unchanged_test() ->
    flush(),
    M = msg(),
    S = (state())#state{batch_size = 1, msg_state = ready},
    _ = ack(enqueue(1, eredis_sub_ssl_client:queue_or_send(M, S))),
    receive M -> ok after 0 -> ?assert(false) end,
    receive M -> ok after 0 -> ?assert(false) end,
    receive Other -> ?assertEqual(none, Other) after 0 -> ok end.

start_link1_batch_size_test() ->
    meck:new(eredis_sub_ssl_client, [passthrough]),
    meck:expect(eredis_sub_ssl_client, start_link, fun(_, _, _, _, _, _, N) -> {ok, N} end),
    try
        ?assertEqual({ok, 500}, eredis_sub_ssl:start_link([{batch_size, 500}])),
        ?assertEqual({ok, 1}, eredis_sub_ssl:start_link([])),
        ?assertError(function_clause, eredis_sub_ssl:start_link([{batch_size, 0}]))
    after
        meck:unload(eredis_sub_ssl_client)
    end.
