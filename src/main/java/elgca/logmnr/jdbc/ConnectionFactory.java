package elgca.logmnr.jdbc;

import elgca.logmnr.conf.JdbcConfiguration;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC connections.
 */
@FunctionalInterface
public interface ConnectionFactory {
    /**
     * @param config the configuration with JDBC connection information
     * @return the JDBC connection; may not be null
     * @throws SQLException if there is an error connecting to the database
     */
    Connection connect(JdbcConfiguration config) throws SQLException;
}
