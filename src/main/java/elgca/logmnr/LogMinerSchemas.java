package elgca.logmnr;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogMinerSchemas {
    public enum LogMnrOptions {
        DICT_FROM_ONLINE_CATALOG,
        DICT_FROM_REDO_LOGS,
        CONTINUOUS_MINE,
        COMMITTED_DATA_ONLY,
        SKIP_CORRUPTION,
        NO_SQL_DELIMITER,
        PRINT_PRETTY_SQL,
        NO_ROWID_IN_STMT,
        DDL_DICT_TRACKING
    }

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
    public static String getSelectLogMnrContentsSQL() {
        return "SELECT " +
                Arrays.stream(LogMnrContents.values())
                        .map(LogMnrContents::toString)
                        .reduce((a, b) -> a + ", " + b)
                        .get()
                + " FROM V$LOGMNR_CONTENTS "
                ;
    }

    public static String getSupportedOperations(Operation... op) {
        return "OPERATION_CODE IN (" +
                Arrays.stream(op)
                        .map(x -> String.valueOf(x.code))
                        .reduce((a, b) -> a + ", " + b)
                        .get()
                + ")";
    }

    public static String getStopLogMinerSQL() {
        return "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
    }

    public static String getStartLogMinerSQL(boolean dictFilePath,
                                             LogMnrOptions... options) {
        Stream<LogMnrOptions> stream = Arrays.stream(options);
        List<String> content = new ArrayList<>();
        if (dictFilePath) {
            content.add("DICTFILENAME => :DICTFILENAME");
            stream = stream
                    .filter(x -> {
                        switch (x) {
                            case DICT_FROM_REDO_LOGS:
                            case DICT_FROM_ONLINE_CATALOG:
                                return false;
                            default:
                                return true;
                        }
                    });
        }
        content.add("STARTSCN => :STARTSCN");
//        content.add("ENDSCN => :ENDSCN");
        stream.map(x -> "DBMS_LOGMNR." + x.toString())
                .reduce((a, b) -> String.format("%s+%s", a, b))
                .ifPresent(s -> content.add("OPTIONS => " + s));
        //        content.add("BEGIN\nDBMS_LOGMNR.START_LOGMNR(\n");
        //        content.add(");\nEND;");
        return String.format("BEGIN\nDBMS_LOGMNR.START_LOGMNR(\n%s\n);\nEND;", content
                .stream()
                .reduce((a, b) -> String.format("%s,%s", a, b))
                .get());
    }

    public static String readUTLFilePath() {
        return "select value from v$parameter where name like 'utl_file_dir'";
    }

    public static String buildDictionaryFile(String utlPath, String dictName) {
        return String.format("BEGIN\n dbms_logmnr_d.build(dictionary_filename => '%s',dictionary_location => '%s');\nEND;",
                dictName,
                utlPath
        );
    }

    public static String removeDictionaryFile(String utlPath,String dictName){
        return String.format("BEGIN\n UTL_FILE.FREMOVE(dictionary_filename => '%s',dictionary_location => '%s');\nEND;",
                utlPath,
                dictName
        );
    }

    public static String addLogFile() {
        throw new RuntimeException("not supported");
    }

    public static String getOldestSCN() {
        return "SELECT FIRST_CHANGE#, STATUS from GV$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# > ? ORDER BY FIRST_CHANGE#";
    }

    public static String getCurrentSCN() {
        return "SELECT CURRENT_SCN FROM GV$DATABASE";
    }

    public static String logMinerSelectSql(Set<TableId> tableIds) {
        return String.format("%s WHERE (%s AND %s ) OR %s",
                LogMinerSchemas.getSelectLogMnrContentsSQL(),
                //monitor dml type
                LogMinerSchemas.getSupportedOperations(Operation.INSERT, Operation.UPDATE, Operation.DELETE),
                //monitor table list
                LogMinerSchemas.parseTableWhiteList(tableIds).get(),
                //use local cache, monitor commit and rollback
                LogMinerSchemas.getSupportedOperations(Operation.COMMIT, Operation.ROLLBACK)
        );
    }

    public static Optional<String> parseTableWhiteList(Set<TableId> tableIds) {
        Map<String, List<TableId>> whiteList = tableIds.stream().collect(Collectors.groupingBy(TableId::schema));
        return whiteList.entrySet().stream().map(x ->
                String.format(
                        "(SEG_OWNER='%s' AND TABLE_NAME IN (%s))",
                        x.getKey(),
                        x.getValue().stream().map(t -> String.format("'%s'", t.table()))
                                .reduce((a, b) -> String.format("%s, %s", a, b))
                                .get()
                )
        )
                .reduce((a, b) -> String.format("%s OR %s", a, b))
                .map(c -> String.format("( %s )", c));
    }

}
