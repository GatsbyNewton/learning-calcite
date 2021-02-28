package edu.wzm.db;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
//import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author: wangzhiming
 * @Date: 2020-01-06
 * @version:
 * @Description:
 */
public class MySQLTest {
    private static final Logger LOGGER = LogManager.getLogger(MySQLTest.class);

    public static void main(String[] args) throws Exception {
        MySQLTest test = new MySQLTest();
        test.json();
//        test.config();
    }

    public void configFile() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/test-classes/example-model.json", "foo", "bar")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("select empno, gender, name from EMPS where name = 'John'");
            }
        }
    }

    public void json() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String meta = "inline:{\n" +
                "    version: '1.0',\n" +
                "    defaultSchema: 'test_schema',\n" +
                "    schemas: [\n" +
                "      {\n" +
                "        name: 'test',\n" +
                "        type: 'custom',\n" +
                "        factory: 'org.apache.calcite.adapter.jdbc.JdbcSchema$Factory',\n" +
                "        operand: {\n" +
                "          jdbcDriver: 'com.mysql.jdbc.Driver',\n" +
                "          jdbcUrl: 'jdbc:mysql://localhost:3306/test',\n" +
                "          jdbcUser: 'root',\n" +
                "          jdbcPassword: '12345678'\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        Properties prop = new Properties();
        prop.put("model", meta);
        Connection connection = DriverManager.getConnection("jdbc:calcite:", prop);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from \"test\".\"person\"");

        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
        resultSet.close();
        statement.close();
        connection.close();
    }

//    public void config() throws Exception {
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setUrl("jdbc:mysql://localhost:3306/test");
//        dataSource.setUsername("root");
//        dataSource.setPassword("12345678");
//
//        Properties info = new Properties();
//        info.setProperty("lex", "JAVA");
//        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
//        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
//        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource,
//                null, "test");
//        rootSchema.add("test", schema);
//        Statement statement = calciteConnection.createStatement();
//        ResultSet resultSet = statement.executeQuery(
//                "select * from test.person");
//
//        final StringBuilder buf = new StringBuilder();
//        while (resultSet.next()) {
//            int n = resultSet.getMetaData().getColumnCount();
//            for (int i = 1; i <= n; i++) {
//                buf.append(i > 1 ? "; " : "")
//                        .append(resultSet.getMetaData().getColumnLabel(i))
//                        .append("=")
//                        .append(resultSet.getObject(i));
//            }
//            System.out.println(buf.toString());
//            buf.setLength(0);
//        }
//        resultSet.close();
//        statement.close();
//        connection.close();
//    }
}
