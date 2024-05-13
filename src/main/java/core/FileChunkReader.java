package core;

import java.io.*;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChunkReader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FileChunkReader.class);

    private final long offset;
    private final int chunkSize;
    private final String filePath;
    private final ConcurrentBitSet concurrentBitSet;
    private final Predicate<String> stringValidator;
    private final Function<String, Long> stringConverter;

    private final AtomicLong readIpCounter;

    public FileChunkReader(long offset, int chunkSize, String filePath, ConcurrentBitSet concurrentBitSet,
                           Predicate<String> stringValidator, Function<String, Long> stringConverter,
                           AtomicLong readIpCounter) {
        this.offset = offset;
        this.chunkSize = chunkSize;
        this.filePath = filePath;
        this.concurrentBitSet = concurrentBitSet;
        this.stringValidator = stringValidator;
        this.stringConverter = stringConverter;
        this.readIpCounter = readIpCounter;
    }

    @Override
    public void run() {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            logger.info("Thread {} started reading file: {} with offset: {} and chunk size: {}",
                    Thread.currentThread().getId(), filePath, offset, chunkSize);

            file.seek(offset);
            if (offset != 0) {
                findNextNewLine(file);
            }

            long innerCount = 0;
            while ((file.getFilePointer() <= offset + chunkSize) && file.getFilePointer() < file.length()) {
                String line = readLine(file);
                logger.info("Thread {} read the line: {} on pos: {}", Thread.currentThread().getId(), line,
                        file.getFilePointer() - line.getBytes(StandardCharsets.UTF_8).length);
                readIpCounter.getAndIncrement();
                innerCount++;
                if (stringValidator.test(line)) {
                    long converted = stringConverter.apply(line);
                    boolean added = concurrentBitSet.setIfNotExists(converted);
                    logger.info("Line {} was converted to {} and added: {}", line, converted, added);
                }
            }


            logger.info("Thread {} with offset {} read {} lines", Thread.currentThread().getId(), offset, innerCount);

        } catch (IOException e) {
            String errorMsg = String.format("Error occurred while reading the file: %s with offset: %s", filePath, offset);
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private String readLine(RandomAccessFile file) throws IOException {
        StringBuilder line = new StringBuilder();
        long filePointer = file.getFilePointer();
        long fileLength = file.length();

        while (filePointer < fileLength) {
            char c = (char) file.readByte();
            filePointer = file.getFilePointer();

            if (c == '\n') {
                break;
            }

            line.append(c);
        }

        return line.toString();
    }

    private void findNextNewLine(RandomAccessFile file) throws IOException {
        while (file.read() != '\n') {
            long pos = file.getFilePointer();
            if (pos == file.length()) {
                break;
            }
        }
    }
}
