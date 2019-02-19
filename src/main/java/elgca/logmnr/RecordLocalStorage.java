package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public class RecordLocalStorage {

    private final Map<String, RecordQueue> bufferedRecords = new HashMap<>();
    private final String locate;

    public RecordLocalStorage(String locate) {
        this.locate = locate;
    }

    public RecordQueue getRecordQueue(String txnId) {
        return bufferedRecords.computeIfAbsent(txnId, x -> createTransactionBuffer(txnId));
    }

    public void remove(String txnId) {
        try {
            RecordQueue queue = bufferedRecords.get(txnId);
            queue.close();
            bufferedRecords.remove(txnId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear(){
        bufferedRecords.forEach((x,y) -> {
            try {
                y.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        bufferedRecords.clear();
    }

    private RecordQueue createTransactionBuffer(String txnId) {
        try {
            return locate == null || locate.length() == 0 ? new InMemoryQueue(txnId) :
                    new FileBackedQueue(new File(locate), txnId);
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
}
