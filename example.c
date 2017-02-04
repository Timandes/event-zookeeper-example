#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include <event.h>
#include <zookeeper/zookeeper.h>

int pending_ops = 0;

struct example_event_arg {
    zhandle_t *zh;
    struct event ev;
};

void timeout(int sock, short event, void *arg)
{
    printf("Timeout!\n");
}

void signal_handler(int sock, short event, void *arg)
{
    struct event_base *base;

    base = (struct event_base *)arg;
    printf("Got signal %d\n", sock);

    event_base_loopbreak(base);
}

void example_data_completion(int rc, const char *value, int value_len,
                const struct Stat *stat, const void *data)
{
    printf("Got data: len=%d\n", value_len);
    if (value_len > 0)
        printf("Data=%s\n", value);
}

void write_data_completion(int rc, const struct Stat *stat,
                const void *data)
{
    printf("Write finished\n");
}

void on_writing(int sock, short event, void *arg)
{
    int events = ZOOKEEPER_WRITE;
    struct example_event_arg *fea;
    zhandle_t *zh;
    int r;
    printf("On Writing\n");
    fea = (struct example_event_arg *)arg;
    zh = fea->zh;


    r = zookeeper_process(zh, events);
    if (ZOK != r) {
        printf("Found error %d when trying to process results of zk\n");
        return;
    }
    
    r = zoo_state(zh);
    printf("zstate=%d\n", r);
    if (ZOO_CONNECTED_STATE == r)
        --pending_ops;
}

void on_socket_ready(int sock, short event, void *arg)
{
    int zevents = 0;
    zhandle_t *zh = NULL;
    int r, state_before;
    struct example_event_arg *fea;

    printf("Socket ready\n");

    fea = (struct example_event_arg *)arg;
    zh = fea->zh;

    if (event & EV_READ)
        zevents = ZOOKEEPER_READ;
    else if (event & EV_WRITE)
        zevents = ZOOKEEPER_WRITE;
    else {
        printf("ERROR\n");
        return;
    }
    //printf("zevents=%d\n", zevents);
    state_before = zoo_state(zh);
    
    r = zookeeper_process(zh, zevents);
    if (ZOK != r) {
        printf("Found error %d when trying to process results of zk\n");
        return;
    }

    r = zoo_state(zh);
    printf("zstate=%d\n", r);
    if (ZOO_CONNECTED_STATE == r)
        if (pending_ops > 0
            || r != state_before)
            event_add(&fea->ev, NULL);

    printf("Socket ready: DONE\n");
}

int main()
{
    struct event_base *base;
    struct event *ev;
    struct timeval tv;
    struct event *evTerm;
    zhandle_t *zh;
    int r, zfd, zinterest;
    struct timeval ztv;
    struct event zev;
    char *v = "ABC";
    struct example_event_arg *fea;

    base = event_base_new();

    ev = evtimer_new(base, timeout, NULL);
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    event_base_set(base, ev);
    event_add(ev, &tv);

    evTerm = evsignal_new(base, SIGTERM, signal_handler, base);
    event_base_set(base, evTerm);
    event_add(evTerm, NULL);

    zh = zookeeper_init("127.0.0.1:2181", NULL, 2000, 0, NULL, 0);
    r = zookeeper_interest(zh, &zfd, &zinterest, &ztv);
    if (r != ZOK) {
        printf("Found error %d when trying to get the events that zookeeper is interested in\n");
        return EXIT_FAILURE;
    }

    //printf("zfd=%d\n", zfd);
    //printf("zinterest=%d\n", zinterest);
    fea = (struct example_event_arg *)malloc(sizeof(struct example_event_arg));
    fea->zh = zh;
    event_set(&fea->ev, zfd, EV_WRITE, on_writing, fea);
    event_base_set(base, &fea->ev);

    event_set(&zev, zfd, EV_READ | EV_PERSIST, on_socket_ready, fea);
    event_base_set(base, &zev);
    event_add(&zev, NULL);

    r = zoo_aget(zh, "/brokers/topics/php-error/partitions/0/state", 1, example_data_completion, NULL);
    if (ZOK != r) {
        printf("Found error %d when trying to get the data associated with a node\n");
        return EXIT_FAILURE;
    }
    pending_ops++;
    event_add(&fea->ev, NULL);
    
    r = zoo_aget(zh, "/", 1, example_data_completion, NULL);
    if (ZOK != r) {
        printf("Found error %d when trying to get the data associated with a node\n");
        return EXIT_FAILURE;
    }
    pending_ops++;
    event_add(&fea->ev, NULL);
/*
    r = zoo_aset(zh, "/test", v, 3, -1, write_data_completion, NULL);
    if (ZOK != r) {
        printf("Found error %d when trying to set the data associated with a node\n");
        return EXIT_FAILURE;
    }*/

    event_base_dispatch(base);

    printf("Exiting ...\n");
    free(fea);
    event_free(ev);
    event_free(evTerm);
    
    zookeeper_close(zh);

    return EXIT_SUCCESS;
}
