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

public class FileBackedQueue implements RecordQueue {
    private int size;
    private LogMinerData tail;
    private LogMinerData head;

    public File getFile() {
        return file;
    }

    private File file;
    private FileBackedQueueIterator ite;
    private String txId;

    private DataFileWriter<LogMinerData> underlying;

    public FileBackedQueue(File dir, String txId) throws IOException {
        this.txId = txId;
        Path f = Files.createDirectories(dir.toPath());
        this.file = new File(f.toFile(), txId + ".avro");
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

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }


    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<LogMinerData> iterator() {
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


    @NotNull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }


    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean add(LogMinerData e) {
        if (head == null) head = e;
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
    public boolean remove(Object e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends LogMinerData> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
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
        return head;
    }

    @Override
    public void close() throws IOException {
        underlying.close();
        if (ite != null) ite.close();
        if (file.exists()) {
            Files.deleteIfExists(file.toPath());
        }
    }

    @Override
    public String getTxId() {
        return txId;
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
