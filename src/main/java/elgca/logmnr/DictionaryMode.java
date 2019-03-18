package elgca.logmnr;

public enum DictionaryMode {
    DICT_FROM_ONLINE_CATALOG("Online Catalog"),
    DICT_FROM_REDO_LOGS("Redo Logs"),
    DICT_FROM_UTL_FILE("UTL Dict file");

    private final String label;

    DictionaryMode(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
}
