import core.IpCounter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

public class IpCounterTest {
    @Test
    public void shouldCountUniqueIps() {
        URL url = getClass().getClassLoader().getResource("ips0.txt");
        IpCounter counter = new IpCounter(url.getPath(), 10);
        long uniques = counter.countUniques();
        long all = counter.getAllNumbersRead();
        System.out.println("Uniques: " + uniques);
        System.out.println("All: " + all);
        System.out.println("File size: " + new File(url.getPath()).length());
    }
}
