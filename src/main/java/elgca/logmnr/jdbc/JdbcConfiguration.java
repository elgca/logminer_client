package elgca.logmnr.jdbc;

import java.util.Map;
import java.util.Optional;

public class JdbcConfiguration {
    private String hostname;
    private int port;
    private String database;
    private String user;
    private String password;
    private Map<Object, Object> config;

    public JdbcConfiguration(String hostname, int port, String database, String user, String password) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public JdbcConfiguration(Map<Object, Object> properties) {
        this.config = properties;
        this.hostname = getString("jdbc.hostname");
        this.port = getOptional("jdbc.port").map(Integer::valueOf).orElse(1521);
        this.database = getString("jdbc.database");
        this.user = getString("jdbc.user");
        this.password = getString("jdbc.password");
    }

    Optional<String> getOptional(String name) {
        return Optional.ofNullable((String) config.get(name));
    }

    String get(String name) {
        return (String) config.get(name);
    }

    String getString(String name) {
        return get(name);
    }

    Integer getInt(String name) {
        return getOptional(name)
                .map(Integer::valueOf)
                .orElse(null);
    }

    Long getLong(String name) {
        return getOptional(name)
                .map(Long::valueOf)
                .orElse(null);
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }


}
