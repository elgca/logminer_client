package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class RecordQueueTest {

    @Test
    public void recordStorageTest() throws Exception {
        RecordLocalStorage disk = new RecordLocalStorage("local-cache");
        RecordLocalStorage mem = new RecordLocalStorage("");
        String txId = "666";
        RecordQueue dq = disk.getRecordQueue(txId);
        RecordQueue mq = mem.getRecordQueue(txId);
        assert dq instanceof FileBackedQueue;
        assert mq instanceof InMemoryQueue;
        for (long i = 0; i++ < 100000; ) {
            LogMinerData log = new LogMinerData(i, null, "1353", 1, System.currentTimeMillis());
            dq.add(log);
            mq.add(log);
        }
        Thread.sleep(1000);
        Iterator<LogMinerData> it1 = dq.iterator();
        Iterator<LogMinerData> it2 = mq.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            LogMinerData a = it1.next();
            LogMinerData b = it2.next();
            assert a.equals(b);
        }
    }

    @Test
    public void inMemoryTest() {

    }

    @Test
    public void fileBackedTest() {

    }
}