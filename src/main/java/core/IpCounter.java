package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class IpCounter {
    private static final Logger logger = LoggerFactory.getLogger(IpCounter.class);
    private static final Pattern IP_PATTERN = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$");
     private final String filePath;
    private final int threadAmount;
    private final int fileChunkSize;
    private final ConcurrentBitSet ips = new ConcurrentBitSet();
    private final AtomicLong readIpCounter = new AtomicLong(); // all ips number read, for debugging purposes

    public IpCounter(String filePath, int threadAmount, int fileChunkSize) {
        this.filePath = filePath;
        this.threadAmount = threadAmount;
        this.fileChunkSize = fileChunkSize;
    }

    public long countUniques() {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(threadAmount * 2);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threadAmount,
                threadAmount * 2,
                5000,
                TimeUnit.MILLISECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        long fileSize = new File(filePath).length();
        logger.info("File size: {}", fileSize);
        long currentOffset = 0;
        while (currentOffset < fileSize) {
            executor.submit(new FileChunkReader(currentOffset, fileChunkSize, filePath, ips,
                    this::validateIP, this::convertIpToOrdinal, readIpCounter));
            logger.info("Task with offset: {} was submitted, task count: {}", currentOffset, executor.getTaskCount());
            currentOffset += fileChunkSize;
        }

        try {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            logger.error("Thread {} was interrupted while waiting for task termination", Thread.currentThread().getId(), e);
            throw new IllegalStateException("Error occurred while waiting for tasks termination", e);
        }

        return ips.getCardinality();
    }

    public long getAllNumbersRead() {
        return readIpCounter.get();
    }

    public boolean validateIP(String ip) {
        return IP_PATTERN.matcher(ip.trim()).matches();
    }

    public Long convertIpToOrdinal(String ip) {
        String[] ipSegments = ip.split("\\.");
        long result = 0;
        for (String segment : ipSegments) {
            int intSegment = Integer.parseInt(segment);
            result = (result << 8) + intSegment;
        }
        return result;
    }
}
