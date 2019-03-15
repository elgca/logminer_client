package elgca.logmnr.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnectionFactory implements ConnectionFactory{
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectionFactory.class);

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        try {
            String hostName = config.getHostname();
            int port = config.getPort();
            String database = config.getDatabase();
            String user = config.getUser();
            String password = config.getPassword();
            LOGGER.info("connect to oracle : {}", "jdbc:oracle:thin:@" + hostName + ":" + port + "/" + database);
            return DriverManager.getConnection(
                    "jdbc:oracle:thin:@" + hostName + ":" + port + "/" + database, user, password
            );
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }
}
