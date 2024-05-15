package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileChunkSequence {
    private static final Logger logger = LoggerFactory.getLogger(FileChunkSequence.class);

    private static final int MAIN_BUFFER_SIZE = 1_048_576 * 2;
    private static final char LINE_BREAK = '\n';

    private long offset;
    private final long fileSize;
    private final String filePath;
    private long memoryReadCounter;

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
            long remainingBytes = Math.min(MAIN_BUFFER_SIZE, file.length() - offset);
            if (remainingBytes == 0) {
                return Optional.empty();
            }

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, remainingBytes);
            memoryReadCounter += buffer.capacity();
            offset += buffer.capacity();
            String stringed = toStr(buffer);

            if (!remainder.isEmpty()) {
                stringed = remainder + stringed;
                remainder = "";
            }

            if (!stringed.isEmpty() && stringed.charAt(stringed.length() - 1) != LINE_BREAK) {
                stringed = removeAllAfterLastBreak(stringed);
            }

            logger.info("Memory read: {} % ; {}/{}", ((double) memoryReadCounter / fileSize) * 100.0, memoryReadCounter, fileSize);
            return Optional.of(stringed);
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

    private String removeAllAfterLastBreak(String source) {
        String[] parts = source.split("\n");
        if (parts.length == 0) {
            return source;
        }

        remainder = parts[parts.length - 1];
        return Arrays.stream(parts, 0, parts.length - 1)
                .collect(Collectors.joining("\n"));
    }

    private String toStr(MappedByteBuffer buffer) {
        StringBuilder innerBuffer = new StringBuilder();
        while (buffer.hasRemaining()) {
            char symbol = (char) buffer.get();
            innerBuffer.append(symbol);
        }
        return innerBuffer.toString();
    }
}
