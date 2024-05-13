package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final String THREAD_NUMBER_PROPERTY = "thread.number";
    private static final String FILE_CHUNK_SIZE_PROPERTY = "file.chunkSizeBit";

    public static void main(String[] args) {
        if (args.length == 0) {
            logger.error("No argument provided");
            throw new IllegalArgumentException("File path should be specified");
        }

        if (Files.notExists(Paths.get(args[0]))) {
            logger.error("Invalid file path: {}", args[0]);
            throw new IllegalArgumentException("Invalid file path provided");
        }

        Properties props = new Properties();
        try (InputStream inputStream = App.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (inputStream == null) {
                logger.error("Cannot find configuration file: config.properties");
                throw new IOException("Cannot find configuration file: config.properties");
            }
            props.load(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Error loading config.properties file", e);
        }
        String filePath = args[0];
        int threadNumber = Integer.parseInt((String) props.get(THREAD_NUMBER_PROPERTY));
        int fileSizeChunk = Integer.parseInt((String) props.get(FILE_CHUNK_SIZE_PROPERTY));
        IpCounter ipCounter = new IpCounter(filePath, threadNumber, fileSizeChunk);

        long startTime = System.nanoTime();
        long uniqueIps = ipCounter.countUniques();
        long endTime = System.nanoTime() - startTime;
        long allIps = ipCounter.getAllNumbersRead();

        System.out.printf("Read %s IPs\n", allIps);
        System.out.printf("Unique IP numbers found: %s\n", uniqueIps);
        System.out.printf("Time elapsed: %s ms\n", endTime / 1_000_000);
    }
}
