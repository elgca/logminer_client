package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public abstract class RecordLocalStorage {

    abstract public BlockingQueue<CommitEvent> getCommittedRecords();

    abstract public RecordQueue getRecordQueue(String xid);

    abstract public void remove(String xid);

    abstract public void clearAll();

    abstract public void addRecord(LogMinerData data);

    abstract public LogMinerData getEarliest(String xid);

    public Iterator<LogMinerData> getRecordIterator(String xid) {
        return getRecordIterator(xid, true);
    }

    abstract public Iterator<LogMinerData> getRecordIterator(String xid, boolean deleteCache);

    public class CommitEvent {
        private Long earliestScn;
        private Long commitScn;
        private String xid;

        public CommitEvent(Long earliestScn, Long commitScn, String xid) {
            this.earliestScn = earliestScn;
            this.commitScn = commitScn;
            this.xid = xid;
        }

        public CommitEvent() {
        }

        public Long getEarliestScn() {
            return earliestScn;
        }

        public void setEarliestScn(Long earliestScn) {
            this.earliestScn = earliestScn;
        }

        public Long getCommitScn() {
            return commitScn;
        }

        public void setCommitScn(Long commitScn) {
            this.commitScn = commitScn;
        }

        public String getXid() {
            return xid;
        }

        public void setXid(String xid) {
            this.xid = xid;
        }
    }
}
