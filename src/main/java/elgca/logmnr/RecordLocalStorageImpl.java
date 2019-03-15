package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class RecordLocalStorageImpl extends RecordLocalStorage {
    private final Map<String, RecordQueue> bufferedRecords = new ConcurrentHashMap<>();
    private final BlockingQueue<CommitEvent> committedRecords = new LinkedBlockingQueue<>();
    private final String locate;

    public RecordLocalStorageImpl(String locate) {
        this.locate = locate;
    }

    public BlockingQueue<CommitEvent> getCommittedRecords() {
        return committedRecords;
    }

    public RecordQueue getRecordQueue(String xid) {
        return bufferedRecords.computeIfAbsent(xid, x -> createTransactionBuffer(xid));
    }

    public void remove(String xid) {
        try {
            RecordQueue queue = bufferedRecords.get(xid);
            if (queue != null) queue.close();
            bufferedRecords.remove(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearAll() {
        bufferedRecords.forEach((x, y) -> {
            try {
                y.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        committedRecords.clear();
        bufferedRecords.clear();
    }

    private RecordQueue createTransactionBuffer(String xid) {
        try {
            return locate == null || locate.length() == 0 ? new InMemoryQueue(xid) :
                    new FileBackedQueue(new File(locate), xid);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private long getMinScn() {
        return bufferedRecords.values()
                .stream()
                .map(RecordQueue::getEarliest)
                .filter(Objects::nonNull)
                .map(LogMinerData::getScn)
                .filter(Objects::nonNull)
                .min(Long::compareTo)
                .orElse(-1L);
    }

    public void addRecord(LogMinerData data) {
        if (data == null) return;
        String xid = data.getXid();
        switch (LogMinerSchemas.Operation.fromCode(data.getOperation())) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case DDL:
                getRecordQueue(xid).add(data);
                break;
            case COMMIT:
                Long earliest = getMinScn();
                if (bufferedRecords.containsKey(xid)) {
                    committedRecords.add(new CommitEvent(earliest, data.getScn(), data.getXid()));
                }
                break;
            case ROLLBACK:
                remove(xid);
                break;
            default:
                break;
        }
    }

    public LogMinerData getEarliest(String xid) {
        return getRecordQueue(xid).getEarliest();
    }

    public Iterator<LogMinerData> getRecordIterator(String xid, boolean deleteCache) {
        RecordQueue queue = bufferedRecords.get(xid);
        if (queue == null) {
            return Collections.emptyIterator();
        }
        bufferedRecords.remove(xid);
        Iterator<LogMinerData> it = queue.iterator();
        if (deleteCache) {
            return new Iterator<LogMinerData>() {

                @Override
                public boolean hasNext() {
                    if (it.hasNext()) {
                        return true;
                    } else {
                        try {
                            queue.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return false;
                    }
                }

                @Override
                public LogMinerData next() {
                    if (hasNext()) {
                        return it.next();
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        }
        return it;
    }
}
