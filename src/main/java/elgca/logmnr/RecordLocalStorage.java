package elgca.logmnr;

import com.google.common.collect.Iterators;
import elgca.io.logmnr.LogMinerData;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class RecordLocalStorage {

    private final Map<String, RecordQueue> bufferedRecords = new HashMap<>();
    private final String locate;

    public RecordLocalStorage(String locate) {
        this.locate = locate;
    }

    public RecordQueue getRecordQueue(String xid) {
        return bufferedRecords.computeIfAbsent(xid, x -> createTransactionBuffer(xid));
    }

    public void remove(String xid) {
        try {
            RecordQueue queue = bufferedRecords.get(xid);
            queue.close();
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

    long getMinScn() {
        return bufferedRecords.values()
                .stream()
                .map(Queue::peek)
                .filter(Objects::nonNull)
                .map(LogMinerData::getSsn)
                .filter(Objects::nonNull)
                .min(Long::compareTo)
                .orElse(-1L);
    }

    public void addRecord(LogMinerData data) {
        String xid = data.getXid();
        getRecordQueue(xid).add(data);
    }

    public boolean contains(String xid) {
        return bufferedRecords.containsKey(xid);
    }

    public LogMinerData getEarliest(String xid) {
        return getRecordQueue(xid).getEarliest();
    }

    public Iterator<LogMinerData> getRecordIterator(String xid) {
        return getRecordIterator(xid, true);
    }

    public Iterator<LogMinerData> getRecordIterator(String xid, boolean deleteCache) {
        RecordQueue queue = bufferedRecords.get(xid);
        bufferedRecords.remove(xid);
        if (queue == null) {
            return Collections.emptyIterator();
        }
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
