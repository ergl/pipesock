
%% How long to wait between buffer flush
-define(CORK_LEN, 5).

%% How many messages in the buffer before we flush
-define(BUF_WATERMARK, 500).

%% How many bits in each message are reserved as id portion
-define(ID_BITS, 8).
