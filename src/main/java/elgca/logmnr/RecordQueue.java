package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Queue;

public interface RecordQueue extends Queue<LogMinerData>, AutoCloseable {
    abstract String getTxId();

    static RecordQueue fileBacked(String txId) throws IOException {
        return new FileBackedQueue(new File("storage"), txId);
    }

    static RecordQueue inMemory(String txId) throws IOException {
        return new InMemoryQueue(txId);
    }
}
