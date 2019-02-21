package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.util.LinkedList;

public class InMemoryQueue extends LinkedList<LogMinerData> implements RecordQueue {
    private String xid;


    private LogMinerData earliest;

    public InMemoryQueue(String xid) {
        this.xid = xid;
    }

    @Override
    public void close() {
    }

    @Override
    public String getXid() {
        return xid;
    }

    public LogMinerData getEarliest() {
        return earliest;
    }

    @Override
    public boolean add(LogMinerData data) {
        if (earliest == null) earliest = data;
        return super.add(data);
    }
}
