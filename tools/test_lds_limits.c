// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

#include <assert.h>
#include <stdio.h>
#include "../open62541/examples/discovery/ojies_lds_limits.h"

int main(void) {
    unsigned short limit = 0;
    assert(ojies_lds_channel_limit(NULL, &limit) && limit == 256);
    assert(ojies_lds_channel_limit("1", &limit) && limit == 1);
    assert(ojies_lds_channel_limit("128", &limit) && limit == 128);
    assert(ojies_lds_channel_limit("4096", &limit) && limit == 4096);
    const char *invalid[] = {"", "0", "4097", "65536", "-1", "+1",
                             " 256", "256 ", "256x", "999999999999999999999999999"};
    for(size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i)
        assert(!ojies_lds_channel_limit(invalid[i], &limit));
    puts("PASS: finite default, boundaries and invalid channel limits");
    return 0;
}
