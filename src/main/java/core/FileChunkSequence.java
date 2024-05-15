package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class FileChunkSequence {
    private static final Logger logger = LoggerFactory.getLogger(FileChunkSequence.class);

    private static final int MAIN_BUFFER_SIZE = 1_048_576 * 2;
    private static final int ADDITIONAL_BUFFER_SIZE = 1024 * 64;
    private static final char LINE_BREAK = '\n';

    private long offset;
    private final long fileSize;
    private final String filePath;
    private final AtomicLong memoryReadCounter;

    public FileChunkSequence(String filePath, AtomicLong memoryReadCounter) {
        this.filePath = filePath;
        this.memoryReadCounter = memoryReadCounter;
        fileSize = new File(filePath).length();
    }

    public Optional<String> next() {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             FileChannel channel = file.getChannel()) {
            long remainingBytes = Math.min(MAIN_BUFFER_SIZE, file.length() - offset);
            if (remainingBytes == 0) {
                return Optional.empty();
            }

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, remainingBytes);
            memoryReadCounter.getAndAdd(buffer.capacity());
            offset += buffer.capacity();
            String stringed = toStr(buffer);

            // read additional bytes to form correct IP sequence inside one chunk and prevent half-strings
            if (!stringed.isEmpty() && stringed.charAt(stringed.length() - 1) != LINE_BREAK) {
                remainingBytes = Math.min(ADDITIONAL_BUFFER_SIZE, file.length() - offset);
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, remainingBytes);
                String skipped = skipToLineBreak(buffer);
                stringed += skipped;
                offset += buffer.position();
                memoryReadCounter.getAndAdd(buffer.position());
            }

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

    private String skipToLineBreak(MappedByteBuffer buffer) {
        StringBuilder innerBuffer = new StringBuilder();
        while (buffer.hasRemaining()) {
            char symbol = (char) buffer.get();
            if (symbol == '\n') {
                break;
            }
            innerBuffer.append(symbol);
        }
        return innerBuffer.toString();
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
