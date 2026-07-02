/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.neuralsearch.jni.NativeLibrary;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LRU cache for native indices loaded from segment files, backed by Guava {@link Cache}.
 * Entries are evicted in least-recently-used order when the total cached weight
 * would exceed the configured byte limit.
 *
 * Each cache entry is reference-counted. Query threads acquire an {@link IndexLease}
 * which increments the ref count, preventing native memory from being freed while
 * in use. Eviction removes the entry from the cache and decrements the ref count;
 * the native memory is freed only when the last lease is closed (ref count reaches zero).
 */
@Log4j2
public class NativeCacheManager {

    private static NativeCacheManager INSTANCE;
    private static final String KEY_DELIMITER = "@";

    private static final int BYTES_PER_KILOBYTE = 1024;

    private volatile Cache<String, CacheEntry> cache;

    static class CacheEntry {
        final long indexAddress;
        final long sizeBytes;
        final String key;
        // Reference count tracking. The cache holds one reference, each lease holds one.
        // When it reaches 0, native memory is freed.
        final AtomicInteger refCount = new AtomicInteger(1);

        CacheEntry(String key, long indexAddress, long sizeBytes) {
            this.key = key;
            this.indexAddress = indexAddress;
            this.sizeBytes = sizeBytes;
        }

        void incRef() {
            int count;
            do {
                count = refCount.get();
                if (count <= 0) {
                    throw new IllegalStateException("Cannot acquire lease: index " + key + " has been freed");
                }
            } while (!refCount.compareAndSet(count, count + 1));
        }

        void decRef() {
            int newCount = refCount.decrementAndGet();
            if (newCount == 0) {
                NativeLibrary.freeIndex(indexAddress);
                log.debug("Freed native index: key={}", key);
            } else if (newCount < 0) {
                log.error("Ref count went negative for key={}, this is a bug", key);
            }
        }
    }

    /**
     * A lease on a cached native index. Holds a reference that prevents the native
     * memory from being freed. Must be closed after use, typically via try-with-resources.
     */
    public static class IndexLease implements Closeable {
        private final CacheEntry entry;
        private boolean closed = false;

        private IndexLease(CacheEntry entry) {
            this.entry = entry;
        }

        public long getIndexAddress() {
            if (closed) {
                throw new IllegalStateException("IndexLease already closed");
            }
            return entry.indexAddress;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                entry.decRef();
            }
        }
    }

    public static synchronized NativeCacheManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new NativeCacheManager();
        }
        return INSTANCE;
    }

    public static String constructCacheKey(String engineFileName, SegmentInfo segmentInfo) {
        final String segmentId = Base64.getEncoder().encodeToString(segmentInfo.getId());
        final String cacheKey = engineFileName + KEY_DELIMITER + segmentId;
        return cacheKey;
    }

    public static String extractIndexFileName(final String cacheKey) {
        final int indexOfDelimiter = cacheKey.lastIndexOf(KEY_DELIMITER);
        if (indexOfDelimiter != -1) {
            final String vectorFileName = cacheKey.substring(0, indexOfDelimiter);
            return vectorFileName;
        }
        return null;
    }

    private NativeCacheManager() {
        this.cache = buildCache(0);
    }

    private Cache<String, CacheEntry> buildCache(long limitBytes) {
        return CacheBuilder.newBuilder()
            .maximumWeight(limitBytes / BYTES_PER_KILOBYTE)
            .weigher((Weigher<String, CacheEntry>) (key, entry) -> (int) (entry.sizeBytes / BYTES_PER_KILOBYTE + 1))
            .removalListener((RemovalListener<String, CacheEntry>) notification -> {
                CacheEntry evicted = notification.getValue();
                if (evicted != null) {
                    log.debug(
                        "Evicted index from cache: key={}, size={}, cause={}",
                        notification.getKey(),
                        evicted.sizeBytes,
                        notification.getCause()
                    );
                    evicted.decRef();
                }
            })
            .build();
    }

    /**
     * Update the cache size limit. Rebuilds the cache, migrating existing entries.
     * Entries that exceed the new limit will be evicted by Guava's LRU policy.
     */
    public synchronized void updateCacheLimit(long bytes) {
        Cache<String, CacheEntry> oldCache = this.cache;
        Cache<String, CacheEntry> newCache = buildCache(bytes);
        // Increment ref counts BEFORE putAll so that entries evicted by Guava
        // during migration still have the correct count (the new cache's removal
        // listener will decRef, balancing this incRef).
        for (CacheEntry entry : oldCache.asMap().values()) {
            entry.incRef();
        }
        newCache.putAll(oldCache.asMap());
        this.cache = newCache;
        // Invalidate old cache to release its references.
        oldCache.invalidateAll();
    }

    /**
     * Acquire a lease on the native index for the given segment file path.
     * The returned {@link IndexLease} holds a reference that prevents the native memory
     * from being freed. Callers must close the lease after use (typically via try-with-resources).
     *
     * @param key the segment file path (used as cache key and passed to {@link NativeLibrary#loadIndex})
     * @param directory the Lucene directory containing the index file
     * @return an {@link IndexLease} providing the native index address
     * @throws IOException if the file size cannot be read or the index cannot be loaded
     */
    public IndexLease acquireIndex(String key, Directory directory) throws IOException {
        // Fast path: try to acquire from the cache without locking.
        CacheEntry entry = cache.getIfPresent(key);
        if (entry != null) {
            try {
                entry.incRef();
                return new IndexLease(entry);
            } catch (IllegalStateException e) {
                // Entry was concurrently evicted and freed; fall through to synchronized load.
            }
        }

        // Slow path: synchronize to prevent duplicate native index loads for the same key.
        synchronized (this) {
            // Double-check: another thread may have loaded it while we waited for the lock.
            entry = cache.getIfPresent(key);
            if (entry != null) {
                try {
                    entry.incRef();
                    return new IndexLease(entry);
                } catch (IllegalStateException e) {
                    // Entry was freed between lookup and incRef; proceed to load.
                }
            }

            // Load a fresh entry with refCount=2
            // (1 for the cache's reference + 1 for the caller's lease).
            // This ensures that even if Guava evicts the entry immediately upon put(),
            // the caller's reference keeps the native memory alive until the lease is closed.
            CacheEntry newEntry = loadEntry(key, directory);
            newEntry.refCount.set(2);
            cache.put(key, newEntry);
            return new IndexLease(newEntry);
        }
    }

    private synchronized CacheEntry loadEntry(String cacheKey, Directory directory) throws IOException {
        String fileName = extractIndexFileName(cacheKey);
        long fileSize = directory.fileLength(fileName);
        String fullPath = resolveFullPath(directory, fileName);
        try {
            long indexAddress = NativeLibrary.loadIndex(fullPath);
            log.debug("Loaded index into cache: key={}, size={}", cacheKey, fileSize);
            return new CacheEntry(cacheKey, indexAddress, fileSize);
        } catch (Exception e) {
            log.error("Failed to load native index file: {}", fullPath, e);
            throw e;
        }
    }

    /**
     * Unwrap any FilterDirectory layers and resolve the full filesystem path for the given file name.
     */
    private static String resolveFullPath(Directory directory, String fileName) throws IOException {
        Directory unwrapped = directory;
        while (unwrapped instanceof FilterDirectory) {
            unwrapped = ((FilterDirectory) unwrapped).getDelegate();
        }
        if (unwrapped instanceof FSDirectory) {
            return ((FSDirectory) unwrapped).getDirectory().resolve(fileName).toString();
        }
        throw new IOException("Cannot resolve filesystem path from directory type: " + directory.getClass().getName());
    }

    /**
     * Remove a specific entry from the cache and release the cache's reference.
     * Native memory is freed when all active leases are closed.
     */
    public void invalidate(String key) {
        cache.invalidate(key);
    }

    /**
     * Evict all entries and release the cache's references.
     * Native memory for each entry is freed when all its active leases are closed.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }
}
