package elgca.logmnr;

public class TableId {
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        assert this.tableName != null;
    }

    public String table() {
        return tableName;
    }

    public String catalog() {
        return catalogName;
    }

    public String schema() {
        return schemaName;
    }

}
