package core;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Task that is supposed to be pushed onto thread pool queue;
 * Handles one file chunk;
 * */
public class FileChunkHandlerTask implements Runnable {
    private final String chunk;
    private final ConcurrentBitSet concurrentBitSet;
    private final Predicate<String> stringValidator;
    private final Function<String, Long> stringConverter;
    private final AtomicLong totalStringCounter;

    public FileChunkHandlerTask(String chunk, ConcurrentBitSet concurrentBitSet, Predicate<String> stringValidator,
                                Function<String, Long> stringConverter, AtomicLong totalStringCounter) {
        this.chunk = chunk;
        this.concurrentBitSet = concurrentBitSet;
        this.stringValidator = stringValidator;
        this.stringConverter = stringConverter;
        this.totalStringCounter = totalStringCounter;
    }

    @Override
    public void run() {
        String[] parts = chunk.split("\n");
        Arrays.stream(parts).forEach(ip -> {
            totalStringCounter.getAndIncrement();
            if (stringValidator.test(ip)) {
                long converted = stringConverter.apply(ip);
                concurrentBitSet.setIfNotExists(converted);
            }
        });
    }
}
