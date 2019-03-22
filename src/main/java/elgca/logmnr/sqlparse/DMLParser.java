package elgca.logmnr.sqlparse;

import elgca.logmnr.LogMinerSchemas.Operation;
import elgca.logmnr.TableId;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DMLParser {

    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");

    private static Object parseValue(Expression value) {
        Object res;
        if (value instanceof DateTimeLiteralExpression) {
            res = parseValue(new TimestampValue(((DateTimeLiteralExpression) value).getValue()));
        } else if (value instanceof NullValue) {
            res = null;
        } else if (value instanceof DoubleValue) {
            res = new BigDecimal(value.toString());
        } else if (value instanceof LongValue) {
            res = new BigDecimal(value.toString());
        } else if (value instanceof DateValue) {
            res = Timestamp.from(((DateValue) value).getValue().toInstant());
        } else if (value instanceof TimeValue) {
            res = Timestamp.from(((TimeValue) value).getValue().toInstant());
        } else if (value instanceof TimestampValue) {
            res = ((TimestampValue) value).getValue();
        } else if (value instanceof StringValue) {
            res = ((StringValue) value).getValue();
        } else if (value instanceof Function) {
            switch (((Function) value).getName()) {
                case "TO_DATE":
                case "TO_TIMESTAMP":
                    String timestamp = ((StringValue) ((Function) value).getParameters().getExpressions().get(0)).getValue();
                    res = LocalDateTime.parse(timestamp, formatter);
                    break;
                case "TO_NUMBER":
                    String number = ((StringValue) ((Function) value).getParameters().getExpressions().get(0)).getValue();
                    res = new BigDecimal(number);
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported function:" +
                            ((Function) value).getName() + ", input: "
                            + value.toString());
            }
        } else {
            throw new IllegalArgumentException(
                    "unsupported type: " + value.getClass().getSimpleName() +
                            ", value: " + value.toString());
        }
        return res;
    }

    private static String parseColumn(Expression col) {
        if (col instanceof Column) {
            return cleanString(((Column) col).getColumnName());
        } else {
            throw new IllegalArgumentException("not a Column");
        }
    }

    private static String cleanString(String str) {
        if (str.startsWith("'") && str.endsWith("'")) str = str.substring(1, str.length() - 1);
        if (str.startsWith("\"") && str.endsWith("\"")) str = str.substring(1, str.length() - 1);
        return str;
    }

    public static DMLData parser(String sqlRedo) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse(sqlRedo);
        Operation operation;
        Table table;
        LinkedHashMap<String, Object> oldValues = new LinkedHashMap<>();
        LinkedHashMap<String, Object> newValues = new LinkedHashMap<>();
        if (stmt instanceof Insert) {
            operation = Operation.INSERT;
            Insert insert = (Insert) stmt;
            table = insert.getTable();
            List<Column> columns = insert.getColumns();
            ExpressionList eList = (ExpressionList) insert.getItemsList();
            List<Expression> valueList = eList.getExpressions();
            int i = 0;
            for (Column c : columns) {
                String key = parseColumn(c);
                Object value = parseValue(valueList.get(i));
                newValues.put(key, value);
                i++;
            }
        } else if (stmt instanceof Update) {
            operation = Operation.UPDATE;
            Update update = (Update) stmt;
            List<Table> tables = update.getTables();
            if (tables.size() != 1) {
                throw new IllegalArgumentException("not support multiple tables in update");
            }
            table = tables.get(0);
            update.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr) {
                    String col = parseColumn(expr.getLeftExpression());
                    Object value = parseValue(expr.getRightExpression());
                    oldValues.put(col, value);
                }

                @Override
                public void visit(IsNullExpression expr) {
                    String col = parseColumn(expr.getLeftExpression());
                    oldValues.put(col, null);
                }
            });
            newValues.putAll(oldValues); // newValues包含全部的oldValues
            List<Column> columns = update.getColumns();
            List<Expression> valueList = update.getExpressions();
            int i = 0;
            for (Column c : columns) {
                String key = parseColumn(c);
                Object value = parseValue(valueList.get(i));
                newValues.put(key, value); //更新列
                i++;
            }
        } else if (stmt instanceof Delete) {
            operation = Operation.DELETE;
            Delete delete = (Delete) stmt;
            table = delete.getTable();
            delete.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr) {
                    String col = parseColumn(expr.getLeftExpression());
                    Object value = parseValue(expr.getRightExpression());
                    oldValues.put(col, value);
                }

                @Override
                public void visit(IsNullExpression expr) {
                    String col = parseColumn(expr.getLeftExpression());
                    oldValues.put(col, null);
                }
            });
        } else {
            throw new UnsupportedOperationException();
        }
        return new DMLData(operation, new TableId(table.getSchemaName(), table.getName()), oldValues, newValues);
    }

    static class DMLData {
        private Operation operation;
        private TableId table;
        private Map<String, Object> before;
        private Map<String, Object> after;

        public DMLData(Operation operation, TableId tableId, Map<String, Object> before, Map<String, Object> after) {
            this.operation = operation;
            this.table = tableId;
            this.before = before;
            this.after = after;
        }

        public Operation getOperation() {
            return operation;
        }

        public TableId getTable() {
            return table;
        }

        public Map<String, Object> getBefore() {
            return before;
        }

        public Map<String, Object> getAfter() {
            return after;
        }

        @Override
        public String toString() {
            return "DMLData{" +
                    "operation=" + operation +
                    ", table=" + table +
                    ", before=" + before +
                    ", after=" + after +
                    '}';
        }
    }
}
