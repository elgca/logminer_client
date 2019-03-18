package elgca.logmnr;

import elgca.logmnr.jdbc.ConnectionFactory;
import elgca.logmnr.conf.JdbcConfiguration;
import elgca.logmnr.jdbc.OracleConnectionFactory;

import java.io.FileReader;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

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
        JdbcConfiguration configuration = new JdbcConfiguration(properties);
        ConnectionFactory factory = new OracleConnectionFactory();
        String name = properties.getProperty("name");
        try (Connection connection = factory.connect(configuration)) {
            Set<TableId> tables = Arrays.stream(properties.getProperty("logmnr.tables").split(","))
                    .map(TableId::parse)
                    .collect(Collectors.toSet());
            OffsetStorage offsetStorage = new OffsetStorage() {
                long commitscn = -1;
                long earliestscn = -1;

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
                    name,
                    configuration.getDatabase(),
                    connection,
                    tables,
                    1,
                    offsetStorage,
                    "",//use memory cache
                    DictionaryMode.DICT_FROM_UTL_FILE
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
