package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class InMemoryQueue extends LinkedList<LogMinerData> implements RecordQueue {
    private String xid;

    private volatile boolean iteratorOnce = true;
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

    @NotNull
    @Override
    public synchronized Iterator<LogMinerData> iterator() {
        if(!iteratorOnce) throw new UnsupportedOperationException("iterator only supported Once");
        iteratorOnce = false;
        return super.iterator();
    }

    @Override
    public boolean add(LogMinerData data) {
        if (earliest == null) earliest = data;
        return super.add(data);
    }


    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull T[] a) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean remove(Object e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends LogMinerData> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(LogMinerData e) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public LogMinerData remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogMinerData poll() {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public LogMinerData element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogMinerData peek() {
        return earliest;
    }

}
