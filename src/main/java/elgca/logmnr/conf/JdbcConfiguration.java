package elgca.logmnr.conf;

import java.util.Map;

public class JdbcConfiguration extends AbstractConfiguration {
    private String hostname;
    private int port;
    private String database;
    private String user;
    private String password;

    public JdbcConfiguration(String hostname, int port, String database, String user, String password) {
        super(null);
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public JdbcConfiguration(Map<?, ?> properties) {
        super(properties);
        this.hostname = getString("jdbc.hostname");
        this.port = getOptional("jdbc.port").map(Integer::valueOf).orElse(1521);
        this.database = getString("jdbc.database");
        this.user = getString("jdbc.user");
        this.password = getString("jdbc.password");
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

    @Override
    public String toString() {
        return "JdbcConfiguration{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", user='" + user + '\'' +
                ", password='******'" +
                '}';
    }
}
