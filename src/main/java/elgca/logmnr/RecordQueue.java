package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Queue;

public interface RecordQueue extends Queue<LogMinerData>, AutoCloseable {
    String getXid();

    LogMinerData getEarliest();

    static RecordQueue fileBacked(String xid) throws IOException {
        return new FileBackedQueue(new File("storage"), xid);
    }

    static RecordQueue inMemory(String xid) throws IOException {
        return new InMemoryQueue(xid);
    }
}
