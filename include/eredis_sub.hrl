%% State in eredis_sub_client
-record(state, {
          host :: string() | undefined,
          port :: integer() | undefined,
          password :: binary() | undefined,
          reconnect_sleep :: integer() | undefined | no_reconnect,

          socket :: port() | undefined,
          parser_state :: #pstate{} | undefined,

          %% Channels we should subscribe to
          channels = [] :: [channel()],

          % The process we send pubsub and connection state messages to.
          controlling_process :: undefined | {reference(), pid()},

          % This is the queue of messages to send to the controlling
          % process.
          msg_queue :: eredis_queue(),
          %% queue:len(msg_queue), kept in step so the hot path is O(1).
          msg_queue_len = 0 :: non_neg_integer(),
          %% Highest msg_queue_len since the last gauge report.
          peak_queue_len = 0 :: non_neg_integer(),

          %% When the queue reaches this size, either drop all
          %% messages or exit.
          max_queue_size :: integer() | infinity,
          queue_behaviour :: drop | exit,

          % The msg_state keeps track of whether we are waiting
          % for the controlling process to acknowledge the last
          % message.
          msg_state = need_ack :: ready | need_ack,

          %% Messages per delivery. 1 sends each message bare; above 1
          %% sends {messages, [Msg], Pid}, one ack per batch.
          batch_size = 1 :: pos_integer(),

          interval :: integer() | undefined,
          tref :: reference() | undefined
}).
