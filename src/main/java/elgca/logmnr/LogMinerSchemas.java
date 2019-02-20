package io.debezium.connector.oracle.logminer;

import java.util.Arrays;

public class LogMinerSchemas {
    public enum LogMnrContents {
        SCN("SCN"),
        USERNAME("USERNAME"),
        OPERATION_CODE("OPERATION_CODE"),
        TIMESTAMP("TIMESTAMP"),
        SQL_REDO("SQL_REDO"),
        TABLE_NAME("TABLE_NAME"),
        COMMIT_SCN("COMMIT_SCN "),
        SEQUENCE("SEQUENCE#"),
        CSF("CSF"),
        XIDUSN("XIDUSN"),
        XIDSLT("XIDSLT"),
        XIDSQN("XIDSQN"),
        RS_ID("RS_ID"),
        SSN("SSN"),
        SEG_OWNER("SEG_OWNER"),
        ROLLBACK("ROLLBACK"),
        ROW_ID("ROW_ID");
        private String fieldName;

        public String getFieldName() {
            return fieldName;
        }

        public int index() {
            return ordinal() + 1;
        }

        @Override
        public String toString() {
            return fieldName;
        }

        LogMnrContents(String fieldName) {
            this.fieldName = fieldName;
        }
    }

    public enum Operation {
        INSERT(1),
        UPDATE(2),
        DELETE(3),
        DDL(5),
        COMMIT(7),
        SELECT_FOR_UPDATE(25),
        ROLLBACK(36);

        public final int code;

        Operation(int code) {
            this.code = code;
        }

        public static Operation fromCode(int code) {
            switch (code) {
                case 1:
                    return INSERT;
                case 2:
                    return UPDATE;
                case 3:
                    return DELETE;
                case 5:
                    return DDL;
                case 7:
                    return COMMIT;
                case 25:
                    return SELECT_FOR_UPDATE;
                case 36:
                    return ROLLBACK;
                default:
                    throw new IllegalArgumentException("unknown operation code: " + code);
            }
        }
    }

    //    @SuppressWarnings("unchecked")
    static String getSelectLogMnrContentsSQL() {
        return "SELECT " +
                Arrays.stream(LogMnrContents.values())
                        .map(LogMnrContents::toString)
                        .reduce((a, b) -> a + ", " + b)
                        .get()
                + " FROM V$LOGMNR_CONTENTS "
                ;
    }

    static String getSupportedOperations(Operation... op) {
        return "OPERATION_CODE IN (" +
                Arrays.stream(op)
                        .map(x -> String.valueOf(x.code))
                        .reduce((a, b) -> a + ", " + b)
                        .get()
                + ")";
    }

    static String getStopLogMinerSQL() {
        return "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
    }

    static String getStartLogMinerSQL() {
        return "BEGIN\n" +
                "DBMS_LOGMNR.START_LOGMNR(" +
                "DICTFILENAME => ?" +
                "STARTSCN => ?," +
                "ENDSCN => ?," +
                "OPTIONS =>  " +
                "DBMS_LOGMNR.SKIP_CORRUPTION" + //条过错误出错的redo log
                "+DBMS_LOGMNR.NO_SQL_DELIMITER" +
                "+DBMS_LOGMNR.NO_ROWID_IN_STMT" +
                "+DBMS_LOGMNR.CONTINUOUS_MINE" +
//                "+DBMS_LOGMNR.COMMITTED_DATA_ONLY" + //不使用COMMIT
                "+DBMS_LOGMNR.STRING_LITERALS_IN_STMT" +
//                "+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG" +
                ");\n" +
                "END;";
        /**
         * DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
         * DBMS_LOGMNR.CONTINUOUS_MINE
         * DBMS_LOGMNR.NO_SQL_DELIMITER
         */
    }

    static String getOldestSCN() {
        return "SELECT FIRST_CHANGE#, STATUS from GV$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY FIRST_CHANGE#";
    }

    static String getCurrentSCN() {
        return "SELECT CURRENT_SCN FROM GV$DATABASE";
    }

}
