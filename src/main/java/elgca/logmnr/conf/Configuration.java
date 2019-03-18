package elgca.logmnr.conf;

import elgca.logmnr.DictionaryMode;
import elgca.logmnr.TableId;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration extends AbstractConfiguration{
    public final static String NAME_KEY = "name";
    public final static String DICTIONARY_MODE_KEY = "dictionary.mode";
    public final static String TABLE_LIST_KEY = "logmnr.tables";
    public final static String FETCH_SIZE_KEY = "fetch.size";
    public final static String LOCAL_CACHED_PATH_KEY = "local.cache.path";

    DictionaryMode dictionaryMode;
    String taskName;
    Set<TableId> tableIds;
    int fetchSize;
    String cachePath;
    JdbcConfiguration jdbcConfiguration;

    public Configuration(Map<?, ?> config) {
        super(config);
        this.dictionaryMode = DictionaryMode.valueOf(getString(DICTIONARY_MODE_KEY));
        this.taskName = getString(NAME_KEY);
        this.tableIds = Arrays.stream(getString(TABLE_LIST_KEY).split("[,]")).map(TableId::parse).collect(Collectors.toSet());
        this.fetchSize = getInt(FETCH_SIZE_KEY) == null ? 1 : getInt(FETCH_SIZE_KEY);
        this.cachePath = getString(LOCAL_CACHED_PATH_KEY);
        this.jdbcConfiguration = new JdbcConfiguration(config);
    }

    public DictionaryMode getDictionaryMode() {
        return dictionaryMode;
    }

    public String getTaskName() {
        return taskName;
    }

    public Set<TableId> getTableIds() {
        return tableIds;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public String getCachePath() {
        return cachePath;
    }

    public JdbcConfiguration getJdbcConfiguration() {
        return jdbcConfiguration;
    }
}
