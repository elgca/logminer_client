package elgca.logmnr;

import elgca.logmnr.conf.Configuration;
import elgca.logmnr.jdbc.ConnectionFactory;
import elgca.logmnr.jdbc.OracleConnectionFactory;

import java.io.FileReader;
import java.sql.Connection;
import java.util.Properties;

/**
 * 读取Oracle的redoLog输出到控制台
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String confPath = "conf/config.properties";
        if (args.length > 0) {
            confPath = args[0];
        }
        Properties properties = new Properties();
        try (FileReader fileReader = new FileReader(confPath)) {
            properties.load(fileReader);
        }

        Configuration configuration = new Configuration(properties);
        ConnectionFactory factory = new OracleConnectionFactory();
        try (Connection connection = factory.connect(configuration.getJdbcConfiguration())) {
            OffsetStorage offsetStorage = new OffsetStorage() {
                long commitscn = -1;
                long earliestscn = 1; // from earliest scn

                @Override
                public long getCommitScn() {
                    return commitscn;
                }

                @Override
                public long getEarliestScn() {
                    return earliestscn;
                }

                @Override
                public void setCommitScn(Long scn) {
                    commitscn = scn;
                }

                @Override
                public void setEarliestScn(Long scn) {
                    earliestscn = scn;
                }
            };

            LogMinerReader reader = new LogMinerReader(
                    configuration.getTaskName(),
                    configuration.getJdbcConfiguration().getDatabase(),
                    connection,
                    configuration.getTableIds(),
                    configuration.getFetchSize(),
                    offsetStorage,
                    configuration.getCachePath(),//use memory cache
                    configuration.getDictionaryMode()
            );

            RecordLocalStorageImpl.class.getConstructor(String.class).newInstance("");

            reader.start();
            while (reader.isRunning()) {
                reader.process(x -> {
                    System.out.println(x);
                });
            }
        }

    }
}
