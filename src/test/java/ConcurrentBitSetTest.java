import core.ConcurrentBitSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConcurrentBitSetTest {

    @Test
    public void shouldAddEntriesToSet() {
        ConcurrentBitSet bitSet = new ConcurrentBitSet();
        bitSet.setIfNotExists(1L);
        bitSet.setIfNotExists(0L);
        bitSet.setIfNotExists(4294967295L);
        bitSet.setIfNotExists(1431655765L);
        bitSet.setIfNotExists(1431655766L);
        bitSet.setIfNotExists(4294967295L);
        Assertions.assertEquals(5, bitSet.getCardinality());
    }
}