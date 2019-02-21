package elgca.logmnr;

import com.google.common.collect.Iterators;
import elgca.io.logmnr.LogMinerData;
import elgca.logmnr.LogMinerSchemas.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;
import java.util.Iterator;
import java.util.Set;

import static elgca.logmnr.LogMinerSchemas.LogMnrContents.*;
import static elgca.logmnr.LogMinerSchemas.LogMnrOptions.*;

public class LogMinerReader implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerReader.class);

    private String dictFilePath;
    private String databaseName;
    private Connection connection;
    private Set<TableId> tableIds;
    private boolean closed;
    private int db_fetch_size;
    private long streamOffsetScn;

    private CallableStatement logMinerSelect;
    private CallableStatement startLogMnrStmt;
    private CallableStatement endLogMnrStmt;
    private CallableStatement getOldestSCN;
    private CallableStatement getLatestSCN;

    private ResultSet logMinerData;

    private RecordLocalStorage storage;

    public LogMinerReader(Connection connection,
                          String databaseName,
                          Set<TableId> tableIds,
                          int db_fetch_size,
                          long streamOffsetScn,
                          String dictFilePath,
                          String cachePath
    ) throws SQLException {
        this.connection = connection;
        this.db_fetch_size = db_fetch_size;
        this.tableIds = tableIds;
        this.databaseName = databaseName;
        this.streamOffsetScn = streamOffsetScn;
        this.closed = false;
        this.dictFilePath = dictFilePath;
        storage = new RecordLocalStorage(cachePath);

        String startLogMnr =
                LogMinerSchemas.getStartLogMinerSQL(true,
                        CONTINUOUS_MINE,
                        SKIP_CORRUPTION,
                        NO_SQL_DELIMITER,
                        NO_ROWID_IN_STMT
                );

        String logMinerSelectSql = String.format("%s WHERE (%s AND %s ) OR %s",
                LogMinerSchemas.getSelectLogMnrContentsSQL(),
                //monitor dml type
                LogMinerSchemas.getSupportedOperations(Operation.INSERT, Operation.UPDATE, Operation.DELETE),
                //monitor table list
                LogMinerSchemas.parseTableWhiteList(this.tableIds).get(),
                //use local cache, monitor commit and rollback
                LogMinerSchemas.getSupportedOperations(Operation.COMMIT, Operation.ROLLBACK)
        );

        LOGGER.info(startLogMnr);
        LOGGER.info(logMinerSelectSql);

        startLogMnrStmt = connection.prepareCall(startLogMnr);
        endLogMnrStmt = connection.prepareCall(LogMinerSchemas.getStopLogMinerSQL());
        logMinerSelect = connection.prepareCall(logMinerSelectSql);
        logMinerSelect.setFetchSize(1);
        if(db_fetch_size > 1){
            logMinerSelect.setFetchSize(db_fetch_size);
        }

        getOldestSCN = connection.prepareCall(LogMinerSchemas.getOldestSCN());
        getLatestSCN = connection.prepareCall(LogMinerSchemas.getCurrentSCN());
    }

    private long getEndingSCN() throws SQLException {
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

    public void start() throws SQLException {
        try {
            //archived log中最早的scn号
            long StartSCN = 0L;
            if (streamOffsetScn != 0L) {
                getOldestSCN.setLong(1, streamOffsetScn);
                ResultSet lastScnFirstPosRSet = getOldestSCN.executeQuery();
                while (lastScnFirstPosRSet.next()) {
                    StartSCN = lastScnFirstPosRSet.getLong("FIRST_CHANGE#");
                }
                lastScnFirstPosRSet.close();
                if (StartSCN == 0L) {
                    LOGGER.warn("Could not find SCN from archived_log.SCN position : {}", streamOffsetScn);
                }
            }
            //如果scn号为0,使用当前scn号
            //为0则说明无法再archived_log中找到scn对应的日志文件记录
            if (StartSCN == 0L) {
                throw new IllegalArgumentException("Invalid SCN number : " + streamOffsetScn);
            }
            startLogMnrStmt.setString(1, dictFilePath);
            startLogMnrStmt.setLong(2, StartSCN);
            startLogMnrStmt.setLong(3, getEndingSCN());
            startLogMnrStmt.execute();
            logMinerSelect.setFetchSize(1);
//            logMinerSelect.setFetchSize(this.db_fetch_size > 1 ? this.db_fetch_size : 1);
            //use local buffer,should not set scn
            //logMinerSelect.setLong(1, streamOffsetScn);
            logMinerData = logMinerSelect.executeQuery();
            LOGGER.info("LogMnr started");
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void close() {
        LOGGER.info("LogMnr stopping...");
        closed = true;
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

    public LogMinerData read() throws LogMinerException {
        String sqlX = "";
        try {
            if (!this.closed && logMinerData.next()) {
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
                int rollback = logMinerData.getInt(ROLLBACK.index());
                String rowId = logMinerData.getString(ROW_ID.index());
                String xid = xidUsn + "." + xidSlt + "." + xidSqn;

                String segOwner = logMinerData.getString(SEG_OWNER.index());
                String segName = logMinerData.getString(TABLE_NAME.index());

                boolean contSF = logMinerData.getBoolean(CSF.index());
                String sqlRedo = logMinerData.getString(SQL_REDO.index());
                if (sqlRedo.contains("temporary tables")) {
                    return null;
                }
                while (contSF) {
                    logMinerData.next();
                    sqlRedo += logMinerData.getString(SQL_REDO.index());
                    contSF = logMinerData.getBoolean(CSF.index());
                }
                sqlX = sqlRedo;
                streamOffsetScn = scn;
                return LogMinerData.newBuilder()
                        .setScn(scn)
                        .setCommitScn(commitScn)
                        .setSequence(sequence)
                        .setUsername(username)
                        .setXid(xid)
                        .setDatabaseName(databaseName)
                        .setSegOwner(segOwner)
                        .setSegName(segName)
                        .setSqlRedo(sqlRedo)
                        .setTimeStamp(timeStamp.getTime())
                        .setOperation(operation)
                        .setRsId(rsId)
                        .setSsn(ssn)
                        .setRowId(rowId)
                        .build();
            }
        } catch (Exception e) {
            if (!this.closed) {
                closed = true;
                throw new LogMinerException("read data error:" + sqlX, e);
            } else {
                LOGGER.warn("ERROR after closed:", e);
            }
        }
        return null;
    }

    public void process(EventHandler handler) throws LogMinerException {
        LogMinerData data = read();
        if (data != null) {
            switch (Operation.fromCode(data.getOperation())) {
                case INSERT:
                case DELETE:
                case UPDATE:
                case DDL:
                    storage.addRecord(data);
                    break;
                case COMMIT:
                    //add commitScn to data
                    Iterator<LogMinerData> it =
                            Iterators.transform(
                                    storage.getRecordIterator(data.getXid()),
                                    x -> LogMinerData.newBuilder(data)
                                            .setCommitScn(data.getScn())
                                            .build());
                    it.forEachRemaining(handler::process);
                case ROLLBACK:
                    storage.remove(data.getXid());
            }
        }
    }

    interface EventHandler {
        void process(LogMinerData data);
    }
}
