example:
	gcc example.c -o example -I/usr/local/libevent-2/include -L/usr/local/libevent-2/lib -I/usr/local/libzookeeper-3.5/include -L/usr/local/libzookeeper-3.5/lib -levent -lzookeeper_st -Wl,-R/usr/local/libzookeeper-3.5/lib
