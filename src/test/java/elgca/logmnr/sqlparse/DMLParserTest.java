package elgca.logmnr.sqlparse;

import elgca.logmnr.TableId;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class DMLParserTest {
    @Test
    public void parserTest() throws JSQLParserException {
        TableId test1 = new TableId("TEST", "TEST1");
        TableId test2 = TableId.parse("TEST.TEST2");

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");
        String insert = "insert into \"TEST\".\"TEST1\"(\"ID\",\"NAME\",\"CREATE_DATE\",\"WALLET\",\"LASTLOGIN\") " +
                "values ('1','test', TO_DATE('2019-03-18 11:24:52', 'YYYY-MM-DD HH24:MI:SS')," +
                "'15235.0',TO_TIMESTAMP('2019-03-18 11:24:52.156785'))";
        DMLParser.DMLData data = DMLParser.parser(insert);
        HashMap<String, Object> after1 = new HashMap<>();
        after1.put("ID", "1");
        after1.put("NAME", "test");
        after1.put("CREATE_DATE", LocalDateTime.parse("2019-03-18 11:24:52", formatter));
        after1.put("WALLET", "15235.0");
        after1.put("LASTLOGIN", LocalDateTime.parse("2019-03-18 11:24:52.156785", formatter));
        assert data.getTable().equals(test1);
        assert after1.equals(data.getAfter());
        assert data.getBefore().isEmpty();

        String update = "UPDATE \"TEST\".\"TEST2\" SET \"ID\" = '10', \"NAME\" = 'xiaoming' WHERE \"ID\" = " +
                "'10' AND \"NAME\" = 'xiaohua'";
        DMLParser.DMLData dataU = DMLParser.parser(update);
        HashMap<String, Object> before2 = new HashMap<>();
        before2.put("ID","10");
        before2.put("NAME","xiaohua");
        HashMap<String, Object>  after2 = new HashMap<>();
        after2.put("ID","10");
        after2.put("NAME","xiaoming");
        assert test2.equals(dataU.getTable());
        assert before2.equals(dataU.getBefore());
        assert after2.equals(dataU.getAfter());

        String delete = "delete from \"TEST\".\"TEST1\" where \"ID\" = '1' and \"NAME\" = 'test' and " +
                "\"CREATE_DATE\" = TO_DATE('2019-03-18 11:24:52', 'YYYY-MM-DD HH24:MI:SS') and " +
                "\"WALLET\" = '15235.0' and \"LASTLOGIN\" IS NULL\n";
        DMLParser.DMLData dataD = DMLParser.parser(delete);
        HashMap<String,Object> after3 = new HashMap<>(after1);
        after3.put("LASTLOGIN",null);
        assert test1.equals(dataD.getTable());
        assert after3.equals(dataD.getBefore());
        assert dataD.getAfter().isEmpty();
    }
}