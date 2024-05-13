import core.IpCounter;
import org.junit.jupiter.api.Test;

public class IpCounterTest {
    private static final int THREAD_AMOUNT = 10;
    private static final int CHUNK_SIZE = 500;
    @Test
    public void shouldCountUniqueIps() {
        IpCounter counter = new IpCounter("src/test/resources/ips2.txt", THREAD_AMOUNT, CHUNK_SIZE);
        long uniques = counter.countUniques();
        long all = counter.getAllNumbersRead();
        System.out.println("Uniques: " + uniques);
        System.out.println("All: " + all);
    }
}
