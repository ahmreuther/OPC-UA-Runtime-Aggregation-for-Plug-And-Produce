// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

/* Offline LDS record-identity and discovery regression. No sockets are opened.
 * Compile with the generated mdnsd_config.h, 1035.c and -lws2_32 on Windows. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static long fake_usec;
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

static int checks, failures;
static char service[] = "_opcua-tcp._tcp.local.";
static char lds[] = "OJIES-LDS._opcua-tcp._tcp.local.";
static char conveyor[] = "Conveyor._opcua-tcp._tcp.local.";
static char dispenser[] = "Dispenser._opcua-tcp._tcp.local.";

static void check(int ok, const char *name) {
    ++checks;
    if(!ok) {
        ++failures;
        printf("FAIL %s\n", name);
    }
}

/* Use the real serializer/parser, including the decoded zero-length RDATA. */
static void parse(struct message *wire, struct message *parsed) {
    unsigned char packet[MAX_PACKET_LEN];
    int size = message_packet_len(wire);
    assert(size > 0 && size <= MAX_PACKET_LEN);
    memcpy(packet, message_packet(wire), (size_t)size);
    memset(parsed, 0, sizeof(*parsed));
    assert(message_parse(parsed, packet, (size_t)size));
}

static void name_identity(unsigned short type) {
    struct message wire = {0}, parsed;
    mdns_answer_t existing = {0};
    message_an(&wire, service, type, QCLASS_IN, 600);
    message_rdata_name(&wire, conveyor);
    parse(&wire, &parsed);
    assert(parsed.ancount == 1 && parsed.an[0].rdlength == 0);
    existing.name = service;
    existing.type = type;
    existing.rdname = conveyor;
    check(_a_match(&parsed.an[0], &existing), "identical decoded name matches");
    existing.rdname = lds;
    check(!_a_match(&parsed.an[0], &existing), "different decoded name does not match");
}

static void srv_identity(void) {
    struct message wire = {0}, parsed;
    mdns_answer_t existing = {0};
    message_an(&wire, lds, QTYPE_SRV, QCLASS_IN, 600);
    message_rdata_srv(&wire, 1, 2, 4840, "lds.local.");
    parse(&wire, &parsed);
    assert(parsed.an[0].rdlength == 0);
    existing.name = lds;
    existing.type = QTYPE_SRV;
    existing.rdname = "lds.local.";
    existing.srv.priority = 1;
    existing.srv.weight = 2;
    existing.srv.port = 4840;
    check(_a_match(&parsed.an[0], &existing), "identical SRV matches");
    existing.rdname = "other.local.";
    check(!_a_match(&parsed.an[0], &existing), "SRV hostname differs");
    existing.rdname = "lds.local.";
    existing.srv.port = 4841;
    check(!_a_match(&parsed.an[0], &existing), "SRV port differs");
    existing.srv.port = 4840;
    existing.srv.priority = 3;
    check(!_a_match(&parsed.an[0], &existing), "SRV priority differs");
    existing.srv.priority = 1;
    existing.srv.weight = 3;
    check(!_a_match(&parsed.an[0], &existing), "SRV weight differs");
    existing.srv.weight = 2;
}

static void raw_identity(void) {
    unsigned char txt[] = {8,'c','a','p','s','=','L','D','S'};
    unsigned char other[] = {8,'c','a','p','s','=','N','O','N'};
    struct message wire = {0}, parsed;
    mdns_answer_t existing = {0};
    message_an(&wire, lds, QTYPE_TXT, QCLASS_IN, 600);
    message_rdata_raw(&wire, txt, sizeof(txt));
    parse(&wire, &parsed);
    existing.name = lds;
    existing.type = QTYPE_TXT;
    existing.rdlen = sizeof(txt);
    existing.rdata = txt;
    check(_a_match(&parsed.an[0], &existing), "identical TXT still matches");
    existing.rdata = other;
    check(!_a_match(&parsed.an[0], &existing), "different TXT does not match");
    existing.rdlen--;
    check(!_a_match(&parsed.an[0], &existing), "different TXT length does not match");
}

static void input(mdns_daemon_t *d, struct message *wire) {
    struct message parsed;
    struct sockaddr_in sender = {0};
    sender.sin_family = AF_INET;
    sender.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    parse(wire, &parsed);
    assert(mdnsd_in(d, &parsed, (struct sockaddr *)&sender, 5353) == 0);
}

static void output(mdns_daemon_t *d, struct message *wire) {
    struct sockaddr_storage dest = {0};
    unsigned short port;
    dest.ss_family = AF_INET;
    memset(wire, 0, sizeof(*wire));
    (void)mdnsd_out(d, wire, (struct sockaddr *)&dest, &port);
}

static void settle_publication(mdns_daemon_t *d) {
    struct message wire;
    int guard = 0;
    while(d->a_publish || d->a_pause || d->a_now) {
        assert(++guard < 30);
        fake_usec += 2000000;
        output(d, &wire);
    }
}

static void known_answer(int known_index) {
    char *names[] = {lds, conveyor, dispenser};
    struct message query = {0}, wire, parsed;
    mdns_daemon_t *d = mdnsd_new(QCLASS_IN, 1000);
    unsigned seen = 0;
    for(int i = 0; i < 3; ++i) {
        mdns_record_t *r = mdnsd_shared(d, service, QTYPE_PTR, 600);
        mdnsd_set_host(d, r, names[i]);
    }
    settle_publication(d);
    message_qd(&query, service, QTYPE_PTR, QCLASS_IN);
    if(known_index >= 0) {
        message_an(&query, service, QTYPE_PTR, QCLASS_IN, 600);
        message_rdata_name(&query, names[known_index]);
    }
    input(d, &query);
    fake_usec += 200000;
    output(d, &wire);
    if(wire.ancount) {
        parse(&wire, &parsed);
        for(int j = 0; j < parsed.ancount; ++j)
            for(int i = 0; i < 3; ++i)
                if(parsed.an[j].type == QTYPE_PTR &&
                   !strcmp(parsed.an[j].known.ptr.name, names[i]))
                    seen |= 1u << i;
    }
    unsigned expected = known_index < 0 ? 7u : 7u & ~(1u << known_index);
    check(seen == expected, "known answer suppresses only that service");
    mdnsd_free(d);
}

static void goodbye_isolation(void) {
    mdns_daemon_t *d = mdnsd_new(QCLASS_IN, 1000);
    struct message response = {0}, goodbye = {0};
    response.header.qr = 1;
    message_an(&response, service, QTYPE_PTR, QCLASS_IN, 600);
    message_rdata_name(&response, lds);
    message_an(&response, service, QTYPE_PTR, QCLASS_IN, 600);
    message_rdata_name(&response, conveyor);
    input(d, &response);
    goodbye.header.qr = 1;
    message_an(&goodbye, service, QTYPE_PTR, QCLASS_IN, 0);
    message_rdata_name(&goodbye, conveyor);
    input(d, &goodbye);
    int lds_seen = 0, conveyor_seen = 0;
    struct cached *c = NULL;
    while((c = _c_next(d, c, service, QTYPE_PTR))) {
        lds_seen += !strcmp(c->rr.rdname, lds);
        conveyor_seen += !strcmp(c->rr.rdname, conveyor);
    }
    check(lds_seen == 1 && conveyor_seen == 0,
          "goodbye removes only the named service");
    mdnsd_free(d);
}

int main(void) {
    name_identity(QTYPE_PTR);
    name_identity(QTYPE_NS);
    name_identity(QTYPE_CNAME);
    srv_identity();
    raw_identity();
    for(int i = -1; i < 3; ++i)
        known_answer(i);
    goodbye_isolation();
    printf("checks=%d failures=%d\n", checks, failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
