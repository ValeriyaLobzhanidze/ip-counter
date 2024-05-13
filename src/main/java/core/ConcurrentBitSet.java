package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentBitSet {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentBitSet.class);
    private final static long MAX_BITS = 4294967296L;
    private final static int BITSET_CHUNK_AMOUNT = 3;
    private final static int BITSET_CHUNK_SIZE = (int) (MAX_BITS / BITSET_CHUNK_AMOUNT) + 1;

    private final AtomicLong cardinality = new AtomicLong();
    private final BitSet[] bitSets = new BitSet[BITSET_CHUNK_AMOUNT];
    private final ReadWriteLock[] locks = new ReadWriteLock[BITSET_CHUNK_AMOUNT];

    public ConcurrentBitSet() {
        for (int i = 0; i < BITSET_CHUNK_AMOUNT ; i++) {
            this.bitSets[i] = new BitSet(BITSET_CHUNK_SIZE);
            this.locks[i] = new ReentrantReadWriteLock();
        }
    }

    public boolean setIfNotExists(long bitIndex) {
        int chunkIndex = (int) (bitIndex / BITSET_CHUNK_SIZE);
        int index = (int) (bitIndex % BITSET_CHUNK_SIZE);
        logger.info("IP {} has chunkIndex: {} and index inside that chunk is: {}", bitIndex, chunkIndex, index);
        ReadWriteLock lock = locks[chunkIndex];
        lock.writeLock().lock();

        try {
            if (!bitSets[chunkIndex].get(index)) {
                bitSets[chunkIndex].set(index);
                cardinality.getAndIncrement();
                return true;
            }
        } finally {
            lock.writeLock().unlock();
        }

        return false;
    }

    public long getCardinality() {
        return cardinality.get();
    }
}

