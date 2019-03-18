package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordQueueTest {

    @Test
    public void recordQueueTest() throws Exception {
        RecordLocalStorage disk = new RecordLocalStorageImpl("local-cache");
        RecordLocalStorage mem = new RecordLocalStorageImpl("");
        String txId = "666";
        List<LogMinerData> list = new ArrayList<>();
        RecordQueue dq = disk.getRecordQueue(txId);
        RecordQueue mq = mem.getRecordQueue(txId);
        assert dq instanceof FileBackedQueue;
        assert mq instanceof InMemoryQueue;
        for (long i = 0; i++ < 100000; ) {
            LogMinerData log = LogMinerData.newBuilder()
                    .setScn(i)
                    .setSequence(665)
                    .build();
            dq.add(log);
            mq.add(log);
            list.add(log);
        }
//        Thread.sleep(1000);
        Iterator<LogMinerData> it1 = dq.iterator();
        Iterator<LogMinerData> it2 = mq.iterator();
        Iterator<LogMinerData> it3 = list.iterator();
        while (it1.hasNext()) {
            LogMinerData a = it1.next();
            LogMinerData b = it2.next();
            LogMinerData c = it3.next();
            assert a.equals(b);
            assert a.equals(c);
        }
    }

    @Test
    public void recordStorageTest() {

    }

    @Test
    public void fileBackedTest() {

    }
}