package elgca.logmnr;

import elgca.logmnr.LogMinerSchemas.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;
import java.util.Set;

import static elgca.logmnr.LogMinerSchemas.LogMnrContents.*;

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


    public LogMinerReader(Connection connection,
                          String databaseName,
                          Set<TableId> tableIds,
                          int db_fetch_size,
                          long streamOffsetScn,
                          String dictFilePath
    ) throws SQLException {
        this.connection = connection;
        this.db_fetch_size = db_fetch_size;
        this.tableIds = tableIds;
        this.databaseName = databaseName;
        this.streamOffsetScn = streamOffsetScn;
        this.closed = false;
        this.dictFilePath = dictFilePath;

        startLogMnrStmt = connection.prepareCall(LogMinerSchemas.getStartLogMinerSQL());
        endLogMnrStmt = connection.prepareCall(LogMinerSchemas.getStopLogMinerSQL());
        logMinerSelect = connection.prepareCall(LogMinerSchemas.logMinerSelectSql(tableIds));

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
            //初始化时候scn会自动提供(debezium),为0则说明无法再archived_log中找到scn对应的日志文件记录
            if (StartSCN == 0L) {
                throw new IllegalArgumentException("非法的SCN,无法找到对应的archived log文件: " + streamOffsetScn);
            }
            startLogMnrStmt.setString(1, dictFilePath);
            startLogMnrStmt.setLong(2, StartSCN);
            startLogMnrStmt.setLong(3,getEndingSCN());
            startLogMnrStmt.execute();
            logMinerSelect.setFetchSize(1);
//            logMinerSelect.setFetchSize(this.db_fetch_size > 1 ? this.db_fetch_size : 1);
            //use local buffer,should not set scn
            //logMinerSelect.setLong(1, streamOffsetScn);
            logMinerData = logMinerSelect.executeQuery();
            LOGGER.info("LogMiner启动成功");
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }


    @Override
    public void close() {
        LOGGER.info("停止LogMiner...");
        closed = true;
        if (logMinerData != null) {
            try {
                LOGGER.info("LogMiner session cancel");
                logMinerData.close();
            } catch (Exception e) {
                LOGGER.error("关闭数据连接错误", e);
            }
        }
        if (endLogMnrStmt != null) {
            try {
                endLogMnrStmt.execute();
            } catch (SQLException e) {
                LOGGER.warn("停止LogMiner时出错", e);
            }
        }
    }


    public LogMinerData read() throws LogMinerException {
        String sqlX = "";
        try {
            if (!this.closed && logMinerData.next()) {
                Long scn = logMinerData.getLong(SCN.index());
                String username = logMinerData.getString(USERNAME.index());
                Operation operation = Operation.fromCode(logMinerData.getInt(OPERATION_CODE.index()));
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

                String segOwner = String.valueOf(logMinerData.getString(SEG_OWNER.index()));
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
                return new LogMinerData(
                        scn,
                        commitScn,
                        sequence,
                        username,
                        xid,
                        databaseName,
                        segOwner,
                        segName,
                        sqlRedo,
                        timeStamp,
                        operation,
                        rsId,
                        ssn,
                        rowId);
            }
        } catch (Exception e) {
            if (!this.closed) {
                throw new LogMinerException("redo_sql:" + sqlX, e);
            } else {
                LOGGER.warn("ERROR after closed:", e);
            }
        }
        return null;
    }
}
