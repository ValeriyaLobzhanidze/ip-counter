package core;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Service counting unique IP numbers, specifically optimized for large files;
 *
 * Algorythm has the following steps:
 * - Current thread (producer thread) reads file in chunks sequentially one by one (via FileChunkSequence);
 * - Once new chunk is read it is pushed onto the thread pool (FixedThreadPool) where it gets handled by FileChunkHandlerTask;
 * - FileChunkHandlerTask handle its input chunk like the follows:
 *      - split the chunk into lines, each of which is IP number (we guarantee that all lines are OK with no 'half-read' strings)
 *      - for each line it:
 *              - checks its correctness via validateIP lambda
 *              - convert it to long via convertIpToOrdinal lambda and gets IP's ordinal number
 *              - push it onto ConcurrentBitSet
 * */
public class IpCounter {
    private static final Pattern IP_PATTERN = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$");
    private final String filePath;
    private final int threadAmount;
    private final ConcurrentBitSet ips = new ConcurrentBitSet();
    private final AtomicLong readIpCounter = new AtomicLong(); // all ips number read, for debugging purposes

    public IpCounter(String filePath, int threadAmount) {
        this.filePath = filePath;
        this.threadAmount = threadAmount;
    }

    public long countUniques() {
        FileChunkSequence reader = new FileChunkSequence(filePath);
        FixedThreadPool fixedThreadPool = new FixedThreadPool(threadAmount);
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
