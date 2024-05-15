package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

/**
 * File reader that is supposed to read file sequentially
 * in bunches when requested (when next() method is invoked the next bunch is given);
 *
 * It's convenient for use in 'producer -> consumers' pattern, when single thread sequentially reads file in bunches
 * and pushes them onto queue for further handling;
 * */
public class FileChunkSequence {
    private static final Logger logger = LoggerFactory.getLogger(FileChunkSequence.class);

    private static final int BUFFER_SIZE = 1_048_576 * 2;
    private static final char LINE_BREAK = '\n';

    private long offset;
    private final long fileSize;
    private final String filePath;
    private long memoryReadCounter;

    /**
     * Whenever we read a new file chunk we have a chance to read some ip in invalid state (read the ip line partially);
     * In that case we just cut off that string (anything behind last \n) and store it in 'remainder' field;
     * Next time we read the bunch we add this 'half-string' in the beginning of chunk thus preserve correctness;
     * This way all clients will get only correct strings;
     * */
    private String remainder = "";

    public FileChunkSequence(String filePath) {
        this.filePath = filePath;
        fileSize = new File(filePath).length();
    }

    public Optional<String> next() {
        if (!hasNext()) {
            return getAndResetRemainder();
        }

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             FileChannel channel = file.getChannel()) {
            long remainingBytes = Math.min(BUFFER_SIZE, file.length() - offset);
            if (remainingBytes == 0) {
                return Optional.empty();
            }

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, remainingBytes);
            memoryReadCounter += buffer.capacity();
            offset += buffer.capacity();
            StringBuilder resultBuffer = toStrBuilder(buffer);

            if (!remainder.isEmpty()) {
                resultBuffer.insert(0, remainder); // on really large files (> 100Gb) usage of StringBuilder operations instead of String concatenation proved to make a huge difference in memory usage and time complexity
                remainder = "";
            }

            // handling the situation where we might read strings in 'half' state: we exclude them from the current buffer,
            // but will include them in the very beginning of the next
            if (!resultBuffer.isEmpty() && resultBuffer.charAt(resultBuffer.length() - 1) != LINE_BREAK) {
                resultBuffer = new StringBuilder(removeAllAfterLastBreak(resultBuffer));
            }

            logger.info("Memory read: {} % ; {}/{}", ((double) memoryReadCounter / fileSize) * 100.0, memoryReadCounter, fileSize);
            return Optional.of(resultBuffer.toString());
        } catch (IOException e) {
            String errorMsg = String.format("Error occurred while reading the file: %s with offset: %s", filePath, offset);
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    public boolean hasNext() {
        return offset < fileSize;
    }

    public boolean hasRemainder() {
        return !remainder.isEmpty();
    }

    public Optional<String> getAndResetRemainder() {
        if (!remainder.isEmpty()) {
            String result = remainder;
            remainder = "";
            return Optional.of(result);
        }
        return Optional.empty();
    }

    private String removeAllAfterLastBreak(StringBuilder source) {
        int lastBreak = source.lastIndexOf(String.valueOf(LINE_BREAK));
        if (lastBreak == -1) {
            return source.toString();
        }

        String result = source.substring(0, lastBreak);
        remainder = source.substring(lastBreak + 1);;
        return result;
    }

    private StringBuilder toStrBuilder(MappedByteBuffer buffer) {
        StringBuilder innerBuffer = new StringBuilder();
        while (buffer.hasRemaining()) {
            char symbol = (char) buffer.get();
            innerBuffer.append(symbol);
        }
        return innerBuffer;
    }
}
