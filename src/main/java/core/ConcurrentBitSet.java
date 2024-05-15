package core;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentBitSet {
    private final static long MAX_BITS = 4294967296L;
    private final static int BITSET_CHUNK_AMOUNT = 20;
    private final static int BITSET_CHUNK_SIZE = (int) (MAX_BITS / BITSET_CHUNK_AMOUNT) + 1;

    private final AtomicLong cardinality = new AtomicLong();
    private final Bucket[] buckets = new Bucket[BITSET_CHUNK_AMOUNT];

    public ConcurrentBitSet() {
        for (int i = 0; i < BITSET_CHUNK_AMOUNT; i++) {
            buckets[i] = new Bucket(BITSET_CHUNK_SIZE);
        }
    }

    public void setIfNotExists(long bitIndex) {
        int chunkIndex = (int) (bitIndex / BITSET_CHUNK_SIZE);
        int index = (int) (bitIndex % BITSET_CHUNK_SIZE);
        buckets[chunkIndex].setIfNotExists(index, cardinality);
    }

    public long getCardinality() {
        return cardinality.get();
    }

    private static class Bucket {
        private final BitSet bitSet;
        private final Object lock = new Object();

        public Bucket(int size) {
            this.bitSet = new BitSet(size);
        }

        public void setIfNotExists(int index, AtomicLong cardinality) {
            synchronized (lock) {
                if (!bitSet.get(index)) {
                    bitSet.set(index);
                    cardinality.incrementAndGet();
                }
            }
        }
    }
}

