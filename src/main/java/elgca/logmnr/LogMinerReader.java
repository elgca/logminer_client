package elgca.logmnr;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import elgca.io.logmnr.LogMinerData;
import elgca.logmnr.LogMinerSchemas.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.sql.*;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static elgca.logmnr.LogMinerSchemas.LogMnrContents.*;
import static elgca.logmnr.LogMinerSchemas.LogMnrOptions.*;

public class LogMinerReader implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerReader.class);

    private final String taskName;

    private String dictFilePath;
    private final String databaseName;
    private final OffsetStorage offsetStorage;
    private final Connection connection;
    private final CallableStatement logMinerSelect;
    private final CallableStatement startLogMnrStmt;
    private final CallableStatement endLogMnrStmt;
    private final CallableStatement getOldestSCN;
    private final CallableStatement getLatestSCN;

    private ResultSet logMinerData;

    private final RecordLocalStorage storage;
    private final ExecutorService generationExecutor =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("Oracle LogMnr Data Generator").build());

    private CountDownLatch shutdownLatch;

    public String getTaskName() {
        return taskName;
    }

    /**
     * @param name          app name/dictionary name
     * @param connection    jdbc
     * @param databaseName  for 12g
     * @param tableIds      white list
     * @param db_fetch_size select fetch size
     * @param offsetStorage offsetStorage
     * @param cachePath     null or '' use memory cache
     * @throws SQLException
     */
    public LogMinerReader(String name,
                          Connection connection,
                          String databaseName,
                          Set<TableId> tableIds,
                          int db_fetch_size,
                          OffsetStorage offsetStorage,
                          String cachePath
    ) throws SQLException {
        this.connection = connection;
        taskName = name;
        //alter nls time format
        execute(LogMinerSchemas.NLS_DATE_FORMAT,
                LogMinerSchemas.NLS_TIMESTAMP_FORMAT,
                LogMinerSchemas.NLS_TIMESTAMP_TZ_FORMAT,
                LogMinerSchemas.NLS_NUMERIC_FORMAT);
        // build flat dictionary file
        try (CallableStatement getUtlFilePath = connection.prepareCall(LogMinerSchemas.readUTLFilePath())) {
            String utl_file_path;
            try (ResultSet rs = getUtlFilePath.executeQuery()) {
                if (!rs.next()) {
                    LOGGER.warn("Missing utl_file_path,use tmpdir for dictionary");
                    utl_file_path = "/tmp/logmnr_reader";
                } else {
                    utl_file_path = rs.getString(1);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("utl_file_path SCN IS {} ", utl_file_path);
                    }
                }
            }
            if (utl_file_path != null) {
                String dictName = "logmnr_" + name + ".ora";
                this.dictFilePath = new File(utl_file_path, dictName).getAbsolutePath();
                connection.prepareCall(LogMinerSchemas.buildDictionaryFile(utl_file_path, dictName)).execute();
            }
        }
        this.databaseName = databaseName;
        this.offsetStorage = offsetStorage;
        storage = new RecordLocalStorageImpl(cachePath);

        String startLogMnr =
                LogMinerSchemas.getStartLogMinerSQL(this.dictFilePath != null,
                        CONTINUOUS_MINE,
                        SKIP_CORRUPTION,
                        NO_SQL_DELIMITER,
                        DICT_FROM_ONLINE_CATALOG
//                        NO_ROWID_IN_STMT
                );

        String logMinerSelectSql = String.format("%s WHERE (%s AND %s ) OR %s",
                LogMinerSchemas.getSelectLogMnrContentsSQL(),
                //monitor dml type
                LogMinerSchemas.getSupportedOperations(Operation.INSERT, Operation.UPDATE, Operation.DELETE),
                //monitor table list
                LogMinerSchemas.parseTableWhiteList(tableIds).get(),
                //use local cache, monitor commit and rollback
                LogMinerSchemas.getSupportedOperations(Operation.COMMIT, Operation.ROLLBACK)
        );

        LOGGER.info(startLogMnr);
        LOGGER.info(logMinerSelectSql);

        startLogMnrStmt = connection.prepareCall(startLogMnr);
        endLogMnrStmt = connection.prepareCall(LogMinerSchemas.getStopLogMinerSQL());
        logMinerSelect = connection.prepareCall(logMinerSelectSql);
        logMinerSelect.setFetchSize(1);
        if (db_fetch_size > 1) {
            logMinerSelect.setFetchSize(db_fetch_size);
        }

        getOldestSCN = connection.prepareCall(LogMinerSchemas.getOldestSCN());
        getLatestSCN = connection.prepareCall(LogMinerSchemas.getCurrentSCN());
    }

    private void execute(String... sqls) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
            if(!connection.getAutoCommit()) connection.commit();
        }
    }

    private long getCurrentScn() throws SQLException {
        try (ResultSet rs = getLatestSCN.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("Missing SCN");
            }
            long scn = rs.getLong(1);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("CURRENT LATEST SCN IS {} ", scn);
            }
            return scn;
        }
    }

    public void start() {
        if (isRunning()) {
            throw new LogMinerException("client already running");
        }
        try {
            //archived log中最早的scn号
            long StartSCN = 0L;
            if (offsetStorage.getEarliestScn() > 0L) {
                getOldestSCN.setLong(1, offsetStorage.getEarliestScn());
                ResultSet lastScnFirstPosRSet = getOldestSCN.executeQuery();
                if (lastScnFirstPosRSet.next()) {
                    StartSCN = lastScnFirstPosRSet.getLong("FIRST_CHANGE#");
                }
                lastScnFirstPosRSet.close();
                if (StartSCN == 0L) {
                    LOGGER.warn("Could not find SCN from archived_log.SCN position : {}", offsetStorage);
                }
            }
            //如果scn号为0,使用当前scn号
            //为0则说明无法再archived_log中找到scn对应的日志文件记录
            if (StartSCN == 0L) {
                StartSCN = getCurrentScn();
                LOGGER.warn("Invalid SCN number : " + offsetStorage);
//                throw new IllegalArgumentException("Invalid SCN number : " + offsetStorage);
            }
            startLogMnrStmt.setLong("STARTSCN", StartSCN);
//            startLogMnrStmt.setLong("ENDSCN", StartSCN);
            if (this.dictFilePath != null) {
                startLogMnrStmt.setString("DICTFILENAME", dictFilePath);
            }
//            startLogMnrStmt.setLong(3, getCurrentScn());
            startLogMnrStmt.execute();
            logMinerSelect.setFetchSize(1);

//            logMinerSelect.setFetchSize(this.db_fetch_size > 1 ? this.db_fetch_size : 1);
            //use local buffer,should not set scn
            //logMinerSelect.setLong(1, offsetStorage);
            logMinerData = logMinerSelect.executeQuery();
            LOGGER.info("LogMnr started");
            this.shutdownLatch = new CountDownLatch(1);
            generationExecutor.submit(this::read);
        } catch (SQLException e) {
            throw new LogMinerException(e);
        }
    }

    @Override
    public void close() {
        LOGGER.info("LogMnr stopping...");
        if (this.shutdownLatch != null) {
            this.shutdownLatch.countDown();
        }
        if (logMinerData != null) {
            try {
                LOGGER.info("LogMnr stopping...");
                logMinerData.close();
            } catch (Exception e) {
                LOGGER.error("LogMnr stopping...", e);
            }
        }
        if (endLogMnrStmt != null) {
            try {
                endLogMnrStmt.execute();
            } catch (SQLException e) {
                LOGGER.warn("LogMnr stopping...", e);
            }
        }
    }

    public boolean isRunning() {
        return this.shutdownLatch != null && shutdownLatch.getCount() > 0;
    }

    private void read() {
        try {
            while (isRunning() && logMinerData.next()) {
                Long scn = logMinerData.getLong(SCN.index());
                String username = logMinerData.getString(USERNAME.index());
                int operation = logMinerData.getInt(OPERATION_CODE.index());
                Timestamp timeStamp = logMinerData.getTimestamp(TIMESTAMP.index());
                Long commitScn = logMinerData.getLong(COMMIT_SCN.index());
                int sequence = logMinerData.getInt(SEQUENCE.index());
                String xidUsn = String.valueOf(logMinerData.getLong(XIDUSN.index()));
                String xidSlt = String.valueOf(logMinerData.getString(XIDSLT.index()));
                String xidSqn = String.valueOf(logMinerData.getString(XIDSQN.index()));
                String rsId = logMinerData.getString(RS_ID.index());
                Long ssn = logMinerData.getLong(SSN.index());
//                int rollback = logMinerData.getInt(ROLLBACK.index());
                String rowId = logMinerData.getString(ROW_ID.index());
                String xid = xidUsn + "." + xidSlt + "." + xidSqn;

                String segOwner = logMinerData.getString(SEG_OWNER.index());
                String segName = logMinerData.getString(TABLE_NAME.index());

                boolean contSF = logMinerData.getBoolean(CSF.index());
                String sqlRedo = logMinerData.getString(SQL_REDO.index());
                if (sqlRedo.contains("temporary tables")) {
                    continue;
                }
                while (contSF) {
                    logMinerData.next();
                    sqlRedo += logMinerData.getString(SQL_REDO.index());
                    contSF = logMinerData.getBoolean(CSF.index());
                }
                LogMinerData data = LogMinerData.newBuilder()
                        .setScn(scn)
                        .setCommitScn(commitScn)
                        .setSequence(sequence)
                        .setUsername(username)
                        .setXid(xid)
                        .setSegOwner(segOwner)
                        .setSegName(segName)
                        .setSqlRedo(sqlRedo)
                        .setTimeStamp(timeStamp.getTime())
                        .setOperation(operation)
                        .setRsId(rsId)
                        .setSsn(ssn)
                        .setRowId(rowId)
                        .build();
                storage.addRecord(data);
            }
        } catch (Exception e) {
            if (shutdownLatch.getCount() > 0) {
                throw new LogMinerException("ERROR read data", e);
            } else {
                LOGGER.warn("ERROR after closed:", e);
            }
        } finally {
            shutdownLatch.countDown();
        }
    }

    public void process(EventHandler handler, long startScn) throws InterruptedException {
        String xid = null;
        try {
            RecordLocalStorage.CommitEvent commit = storage.getCommittedRecords().poll(1, TimeUnit.SECONDS);
            if (commit != null) {
                xid = commit.getXid();
                Long commitScn = commit.getCommitScn();
                Long earliestScn = commit.getEarliestScn();
                this.offsetStorage.setEarliestScn(earliestScn);
                this.offsetStorage.setCommitScn(commitScn);
                if (commitScn < startScn) return;
                Iterators.transform(storage.getRecordIterator(xid),
                        x -> LogMinerData.newBuilder(x)
                                .setCommitScn(commitScn)
                                .setEarliestScn(earliestScn)
                                .build()
                ).forEachRemaining(handler::process);
            }
        } finally {
            if (xid != null) storage.remove(xid);
        }
    }

    public void process(EventHandler handler) throws InterruptedException {
        process(handler, -1);
    }

    @FunctionalInterface
    public interface EventHandler {
        void process(LogMinerData data);
    }
}
