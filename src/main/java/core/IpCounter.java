package core;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class IpCounter {
    private static final Pattern IP_PATTERN = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$");
    private final String filePath;
    private final int threadNumber;
    private final ConcurrentBitSet ips = new ConcurrentBitSet();
    private final AtomicLong readIpCounter = new AtomicLong(); // all ips number read, for debugging purposes

    public IpCounter(String filePath, int threadNumber) {
        this.filePath = filePath;
        this.threadNumber = threadNumber;
    }

    public long countUniques() {
        FileChunkSequence reader = new FileChunkSequence(filePath);
        FixedThreadPool fixedThreadPool = new FixedThreadPool(threadNumber);
        while (reader.hasNext() || reader.hasRemainder()) {
            Optional<String> chunk = reader.next();
            if (chunk.isPresent()) {
                FileChunkHandlerTask task = new FileChunkHandlerTask(chunk.get(), ips, this::validateIP, this::convertIpToOrdinal,
                        readIpCounter);
                fixedThreadPool.execute(task);
            }
        }

        fixedThreadPool.shutdown();
        fixedThreadPool.awaitTermination(10, TimeUnit.MINUTES);
        return ips.getCardinality();
    }

    public long getAllNumbersRead() {
        return readIpCounter.get();
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
