package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.util.Queue;

public interface RecordQueue extends Queue<LogMinerData>, AutoCloseable {
    String getXid();

    LogMinerData getEarliest();
}
