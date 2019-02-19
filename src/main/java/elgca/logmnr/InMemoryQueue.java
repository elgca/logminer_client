package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.io.IOException;
import java.util.LinkedList;

public class InMemoryQueue extends LinkedList<LogMinerData> implements RecordQueue {
    private String txId;

    public InMemoryQueue(String txId) {
        this.txId = txId;
    }

    @Override
    public void close() {
    }

    @Override
    public String getTxId() {
        return txId;
    }
}
