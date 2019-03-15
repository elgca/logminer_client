package elgca.logmnr;

import elgca.io.logmnr.LogMinerData;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * File backed list with avro.
 */
public class FileBackedQueue implements RecordQueue {
    private int size;
    private LogMinerData tail;
    private LogMinerData earliest;
    private volatile boolean iteratorOnce = true;

    public File getFile() {
        return file;
    }

    private File file;
    private FileBackedQueueIterator ite;
    private String xid;

    private DataFileWriter<LogMinerData> underlying;

    @Override
    public LogMinerData getEarliest() {
        return earliest;
    }

    public FileBackedQueue(File dir, String xid) throws IOException {
        this.xid = xid;
        Path f = Files.createDirectories(dir.toPath());
        this.file = new File(f.toFile(), xid + ".avro");
        if (file.exists()) Files.deleteIfExists(file.toPath());
        file.deleteOnExit();
        size = 0;
        DatumWriter<LogMinerData> writer = new SpecificDatumWriter<>(LogMinerData.class);
        underlying = new DataFileWriter<>(writer);
        underlying.create(LogMinerData.SCHEMA$, file);
    }

    @SuppressWarnings("unchecked")
    public LogMinerData tail() {
        return tail;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return tail == null;
    }


    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public synchronized Iterator<LogMinerData> iterator() {
        if(!iteratorOnce) throw new UnsupportedOperationException("iterator only supported Once");
        iteratorOnce = false;
        try {
            underlying.close();
            if (ite != null) ite.close();
            ite = new FileBackedQueueIterator();
            return ite;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyIterator();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean add(LogMinerData e) {
        if (earliest == null) earliest = e;
        tail = e;
        try {
            underlying.append(e);
            size++;
        } catch (IOException e1) {
            e1.printStackTrace();
            return false;
        }
        return true;
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

    @Override
    public void close() throws IOException {
        if (underlying != null) underlying.close();
        underlying = null;
        if (ite != null) ite.close();
        ite = null;
        Files.deleteIfExists(file.toPath());
    }

    @Override
    public String getXid() {
        return xid;
    }

    private class FileBackedQueueIterator implements Iterator<LogMinerData>, Closeable {
        DatumReader<LogMinerData> dataDatumReader = new SpecificDatumReader<>(LogMinerData.class);
        DataFileReader<LogMinerData> underlyingReader = new DataFileReader<>(file, dataDatumReader);

        private FileBackedQueueIterator() throws IOException {
        }

        @Override
        public boolean hasNext() {
            boolean has = underlyingReader.hasNext();
            if (!has) {
                try {
                    underlyingReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return has;
        }

        @Override
        @SuppressWarnings("unchecked")
        public LogMinerData next() {
            return underlyingReader.next();
        }

        @Override
        public void close() throws IOException {
            underlyingReader.close();
        }
    }
}
