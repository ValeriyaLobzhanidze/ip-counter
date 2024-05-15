package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final String THREAD_NUMBER_PROPERTY = "thread.number";
    private static final String CONFIG_PROPERTY_FILE_NAME = "config.properties";

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
        try (InputStream inputStream = App.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTY_FILE_NAME)) {
            Objects.requireNonNull(props).load(inputStream);
        } catch (IOException e) {
            String msg = "Error loading config file";
            logger.error(msg);
            throw new IllegalStateException(msg, e);
        }

        String filePath = args[0];
        int threadNumber = Integer.parseInt((String) props.get(THREAD_NUMBER_PROPERTY));
        IpCounter ipCounter = new IpCounter(filePath, threadNumber);

        long startTime = System.nanoTime();
        long uniqueIps = ipCounter.countUniques();
        long endTime = System.nanoTime() - startTime;
        long allIps = ipCounter.getAllNumbersRead();

        System.out.println();
        System.out.printf("Read %s lines\n", allIps);
        System.out.printf("Unique IP numbers found: %s\n", uniqueIps);
        System.out.printf("Time elapsed: %s minutes\n", endTime / 1_000_000.0 / 60_000.0);
    }
}
