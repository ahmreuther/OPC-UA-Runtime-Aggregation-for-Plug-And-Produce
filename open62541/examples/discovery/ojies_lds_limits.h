// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: MPL-2.0

#ifndef OJIES_LDS_LIMITS_H
#define OJIES_LDS_LIMITS_H

#include <errno.h>
#include <stdlib.h>
#include <string.h>

/* Finite headroom for 100 prepared registrations plus discovery clients.
 * This is a deployment limit, not a guarantee for arbitrary traffic. */
#define OJIES_LDS_DEFAULT_CHANNELS 256
#define OJIES_LDS_MAX_CHANNELS 4096

static int
ojies_lds_channel_limit(const char *value, unsigned short *result) {
    if(!value) {
        *result = OJIES_LDS_DEFAULT_CHANNELS;
        return 1;
    }
    if(!value[0] || strspn(value, "0123456789") != strlen(value))
        return 0;
    errno = 0;
    unsigned long parsed = strtoul(value, NULL, 10);
    if(errno || parsed == 0 || parsed > OJIES_LDS_MAX_CHANNELS)
        return 0;
    *result = (unsigned short)parsed;
    return 1;
}

#endif
