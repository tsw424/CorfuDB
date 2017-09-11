package org.corfudb.infrastructure.log;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A SegmentHandle is a range view of consecutive addresses in the log. It contains
 * the address space along with metadata like addresses that are trimmed and pending trims.
 */

@Data
@Slf4j
class SegmentHandle {
    final long segment;
    @NonNull
    final FileChannel logChannel;

    @Getter
    @NonNull
    final FileChannel readChannel;

    @NonNull
    final FileChannel trimmedChannel;

    @NonNull
    final FileChannel pendingTrimChannel;

    @NonNull
    final String fileName;

    final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap();
    final Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());
    volatile int refCount = 0;


    public synchronized void retain() {
        refCount++;
    }

    public synchronized void release() {
        if (refCount == 0) {
            throw new IllegalStateException("refCount cannot be less than 0, segment " + segment);
        }
        refCount--;
    }

    public void close() {
        Set<FileChannel> channels =
                new HashSet(Arrays.asList(logChannel, trimmedChannel, pendingTrimChannel));
        for (FileChannel channel : channels) {
            try {
                channel.force(true);
                channel.close();
                channel = null;
            } catch (Exception e) {
                log.warn("Error closing channel {}: {}", channel.toString(), e.toString());
            }
        }
    }
}