%%
%% Example code for using eredis_sub_ssl
%%
-module(eredis_ssl_examples).

-export([sub_example/0, pub_example/0, receiver/1]).

%% Simple SSL subscription example
sub_example() ->
    {ok, Sub} = eredis_sub_ssl:start_link(),
    Receiver = spawn_link(fun () ->
                                  eredis_sub_ssl:controlling_process(Sub),
                                  eredis_sub_ssl:subscribe(Sub, [<<"foo">>]),
                                  receiver(Sub)
                          end),
    {Sub, Receiver}.

%% Publish example (using regular eredis with SSL option)
pub_example() ->
    {ok, P} = eredis:start_link([{ssl, true}]),
    eredis:q(P, ["PUBLISH", "foo", "bar"]),
    eredis_client:stop(P).

%% Helper function to receive messages
receiver(Sub) ->
    receive
        Msg ->
            io:format("SSL PUBSUB received: ~p~n", [Msg]),
            eredis_sub_ssl:ack_message(Sub),
            ?MODULE:receiver(Sub)
    end. 