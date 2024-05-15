package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class IpCounter {
    private static final Logger logger = LoggerFactory.getLogger(IpCounter.class);
    private static final Pattern IP_PATTERN = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$");
    private final String filePath;
    private final int threadNumber;
    private final ConcurrentBitSet ips = new ConcurrentBitSet();
    private final AtomicLong readIpCounter = new AtomicLong(); // all ips number read, for debugging purposes
    private final AtomicLong memoryReadCounter = new AtomicLong();

    public IpCounter(String filePath, int threadNumber) {
        this.filePath = filePath;
        this.threadNumber = threadNumber;
    }

    public long countUniques() {
        FileChunkSequence reader = new FileChunkSequence(filePath, memoryReadCounter);
        FixedThreadPool fixedThreadPool = new FixedThreadPool(threadNumber);
        long fileSize = new File(filePath).length();
        while (reader.hasNext()) {
            Optional<String> chunk = reader.next();
            if (chunk.isPresent()) {
                FileChunkHandlerTask task = new FileChunkHandlerTask(chunk.get(), ips, this::validateIP, this::convertIpToOrdinal,
                        readIpCounter);
                fixedThreadPool.execute(task);
                logger.info("Memory read: {} %", ((double) memoryReadCounter.get() / fileSize) * 100.0);
            }
        }

        fixedThreadPool.shutdown();
        try {
            fixedThreadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for tasks termination");
        }

        return ips.getCardinality();
    }

    public long getAllNumbersRead() {
        return readIpCounter.get();
    }

    public long getAllMemoryRead() {
        return memoryReadCounter.get();
    }

    public boolean validateIP(String ip) {
        return IP_PATTERN.matcher(ip.trim()).matches();
    }

    public Long convertIpToOrdinal(String ip) {
        String[] ipSegments = ip.trim().split("\\.");
        long result = 0;
        for (String segment : ipSegments) {
            int intSegment = Integer.parseInt(segment);
            result = (result << 8) + intSegment;
        }
        return result;
    }
}
