// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

/* Offline mDNS serialization regression. No sockets or service is started. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static long fake_usec = 0;
static int test_gettimeofday(struct timeval *tv, void *tz) {
    (void)tz;
    tv->tv_sec = 1000 + fake_usec / 1000000;
    tv->tv_usec = fake_usec % 1000000;
    return 0;
}

#define gettimeofday test_gettimeofday
#ifndef MDNSD_IMPLEMENTATION
#define MDNSD_IMPLEMENTATION "../open62541/deps/mdnsd/libmdnsd/mdnsd.c"
#endif
#include MDNSD_IMPLEMENTATION
#undef gettimeofday

struct guarded_message {
    struct message message;
    /* Keep a broken baseline's overwrite inside this allocated test object. */
    unsigned char canary[1024 * 1024];
};

static void conflict(char *name, int type, void *arg) {
    (void)name; (void)type; (void)arg;
    abort();
}

int main(int argc, char **argv) {
    int count = argc > 1 ? atoi(argv[1]) : 100;
    int calls = 0, max_packet = 0, bad = 0;
    mdns_daemon_t *d = mdnsd_new(1, 1000);
    struct guarded_message *guard = calloc(1, sizeof(*guard));
    assert(guard && count > 0 && count <= 1000);
    for(int i = 0; i < count; ++i) {
        char name[100], txt[100];
        snprintf(name, sizeof(name), "C16 public-test-000000-case-b100-r1 s%03d-192-0-2-66._opcua-tcp._tcp.local.", i + 1);
        snprintf(txt, sizeof(txt), "path=/ojies/c16/public-test-000000-case-b100-r1/s%03d/", i + 1);
        mdns_record_t *srv = mdnsd_unique(d, name, QTYPE_SRV, 600, conflict, NULL);
        mdnsd_set_srv(d, srv, 0, 0, (unsigned short)(51001+i), "192.0.2.66.local.");
        mdns_record_t *record = mdnsd_unique(d, name, QTYPE_TXT, 600, conflict, NULL);
        mdnsd_set_raw(d, record, txt, (unsigned short)strlen(txt));
    }
    do {
        struct sockaddr_storage address;
        unsigned short port;
        memset(&address, 0, sizeof(address));
        address.ss_family = AF_INET;
        memset(guard->canary, 0xa5, sizeof(guard->canary));
        (void)mdnsd_out(d, &guard->message, (struct sockaddr *)&address, &port);
        int length = message_packet_len(&guard->message);
        if(length > max_packet) max_packet = length;
        for(size_t j = 0; j < sizeof(guard->canary); ++j) {
            if(guard->canary[j] != 0xa5) { bad = 1; break; }
        }
        if(length > d->frame || length > MAX_PACKET_LEN || bad) break;
        ++calls;
        /* Drain immediate packets at this instant before advancing fake time. */
        if(mdnsd_sleep(d)->tv_sec || mdnsd_sleep(d)->tv_usec)
            fake_usec += 250000;
    } while((d->probing || d->a_publish || d->a_now) && calls < 100000);
    printf("sources=%d calls=%d max_packet=%d frame=%d packet_capacity=%d guard_changed=%d pending=%d\n",
           count, calls, max_packet, d->frame, MAX_PACKET_LEN, bad, d->probing != NULL);
    mdnsd_free(d);
    free(guard);
    return bad || max_packet > 1000 || calls >= 100000 ? 1 : 0;
}
