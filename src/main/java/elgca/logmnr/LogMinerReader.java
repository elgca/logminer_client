package elgca.logmnr;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import elgca.io.logmnr.LogMinerData;
import elgca.logmnr.LogMinerSchemas.Operation;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
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

    private String dictFilePath = null;
    private final String databaseName;
    private final OffsetStorage offsetStorage;
    private final Connection connection;
    private final DictionaryMode mode;
    private final CallableStatement logMinerSelect;
    private final CallableStatement startLogMnrStmt;
    private final CallableStatement endLogMnrStmt;
    private final CallableStatement getOldestSCN;
    private final CallableStatement getLatestSCN;

    private ResultSet logMinerData;

    private long startCommitScn;
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
     * @param databaseName  not used
     * @param pdb           for 12g, not used
     * @param connection    jdbc
     * @param tableIds      white list
     * @param db_fetch_size select fetch size
     * @param offsetStorage offsetStorage
     * @param cachePath     null or '' use memory cache
     * @param mode          logmnr dictionary mode
     * @throws SQLException
     */
    public LogMinerReader(@NotNull String name,
                          @NotNull String databaseName,
                          String pdb,
                          @NotNull Connection connection,
                          @NotNull Set<TableId> tableIds,
                          int db_fetch_size,
                          @NotNull OffsetStorage offsetStorage,
                          String cachePath,
                          @NotNull DictionaryMode mode
    ) throws SQLException {
        assert !tableIds.isEmpty();
        this.connection = connection;
        this.mode = mode;
        this.taskName = name;
        this.databaseName = databaseName;
        this.offsetStorage = offsetStorage;
        this.startCommitScn = this.offsetStorage.getCommitScn();
        this.storage = new RecordLocalStorageImpl(cachePath);
        //alter nls time format
        setNlsFormat();
        // build flat dictionary file
        ArrayList<LogMinerSchemas.LogMnrOptions> options = new ArrayList<>();
        options.add(CONTINUOUS_MINE);
        options.add(SKIP_CORRUPTION);
        options.add(NO_SQL_DELIMITER);
        options.add(NO_ROWID_IN_STMT);
        switch (mode) {
            case DICT_FROM_REDO_LOGS:
                options.add(DICT_FROM_REDO_LOGS);
                break;
            case DICT_FROM_ONLINE_CATALOG:
                options.add(DICT_FROM_ONLINE_CATALOG);
                break;
            case DICT_FROM_UTL_FILE:
                this.dictFilePath = createUTLDictionary(name);
                break;
        }
        String startLogMnr = LogMinerSchemas.getStartLogMinerSQL(mode, options);

        String logMinerSelectSql = String.format("%s WHERE (%s AND %s ) OR %s",
                LogMinerSchemas.getSelectLogMnrContentsSQL(),
                //monitor dml type
                LogMinerSchemas.getSupportedOperations(Arrays.asList(
                        Operation.INSERT,
                        Operation.UPDATE,
                        Operation.DELETE,
                        Operation.DDL)),
                //monitor table list
                LogMinerSchemas.parseTableWhiteList(tableIds).get(),
                //use local cache, monitor commit and rollback
                LogMinerSchemas.getSupportedOperations(Arrays.asList(Operation.COMMIT, Operation.ROLLBACK))
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

    private void setNlsFormat() throws SQLException {
        execute(LogMinerSchemas.NLS_DATE_FORMAT,
                LogMinerSchemas.NLS_TIMESTAMP_FORMAT,
                LogMinerSchemas.NLS_TIMESTAMP_TZ_FORMAT,
                LogMinerSchemas.NLS_NUMERIC_FORMAT);
    }

    private String createUTLDictionary(String name) throws SQLException {
        try (CallableStatement getUtlFilePath = connection.prepareCall(LogMinerSchemas.readUTLFilePath())) {
            String utl_file_path;
            try (ResultSet rs = getUtlFilePath.executeQuery()) {
                if (!rs.next()) {
                    LOGGER.warn("Missing utl_file_path,use tmpdir for dictionary");
                    utl_file_path = "/tmp";
                } else {
                    utl_file_path = rs.getString(1);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("utl_file_path IS {} ", utl_file_path);
                    }
                }
            }
            if (utl_file_path == null || utl_file_path.length() == 0) {
                return null;
            }
            String dictName = "logmnr_" + name + ".ora";
            connection.prepareCall(LogMinerSchemas.buildDictionaryFile(utl_file_path, dictName)).execute();
            return new File(utl_file_path, dictName).getAbsolutePath();
        }
    }

    private void execute(String... sqls) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
            if (!connection.getAutoCommit()) connection.commit();
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
            if (offsetStorage.getEarliestScn() >= 0L) {
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
            //thread safe running flag
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
        process(handler, startCommitScn);
    }

    private int getDBVersion() {
        try (Statement statement = connection.createStatement();
             ResultSet versionSet = statement.executeQuery("SELECT version FROM product_component_version")) {
            if (versionSet.next()) {
                String versionStr = versionSet.getString("version");
                if (versionStr != null) {
                    int majorVersion = Integer.parseInt(versionStr.substring(0, versionStr.indexOf('.')));
                    LOGGER.info("Oracle Version is " + majorVersion);
                    return majorVersion;
                }
            }
        } catch (SQLException ex) {
            LOGGER.error("Error while getting db version info", ex);
        }
        return -1;
    }

    @FunctionalInterface
    public interface EventHandler {
        void process(LogMinerData data);
    }
}
