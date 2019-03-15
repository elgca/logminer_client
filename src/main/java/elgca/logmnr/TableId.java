package elgca.logmnr;

import java.util.Objects;

public class TableId {
    private final String schemaName;
    private final String tableName;
    private final String id;

    public static TableId parse(String tableId) {
        String[] list = tableId.trim().split("[.]");
        if (list.length != 2) {
            throw new IllegalArgumentException("Illegal table name:" + tableId);
        }
        return new TableId(list[0], list[1]);
    }

    public TableId(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        id = this.schemaName + "." + this.tableName;
        assert this.tableName != null;
    }

    public String table() {
        return tableName;
    }


    public String schema() {
        return schemaName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableId tableId = (TableId) o;
        return Objects.equals(id, tableId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString() {
        return id;
    }
}
