package edu.wzm.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ParseSql {

    private static final Logger LOGGER = LogManager.getLogger(ParseSql.class);

    public static void main(String[] args) throws SqlParseException {
        String sql = "select\n" +
                "    stu.id, stu.name, tch.name\n" +
                "from (\n" +
                "    select id, name\n" +
                "    from student\n" +
                "    where age > 18\n" +
                ") stu\n" +
                "left outer join (\n" +
                "    select id, s_id, name\n" +
                "    from teacher\n" +
                "    where gender = 1\n" +
                ") tch\n" +
                "on stu.id = tch.s_id ";

        SqlParser parser = SqlParser.create(sql);
        SqlSelect sqlSelect = (SqlSelect) (parser.parseStmt());
        SqlNodeList nodeList = sqlSelect.getSelectList();
        nodeList.forEach(col -> System.out.println("Column: " + col));

        SqlJoin node = (SqlJoin) sqlSelect.getFrom();
        if (node.getKind() == SqlKind.JOIN) {
            SqlBasicCall left =  (SqlBasicCall) node.getLeft();
            System.out.println("\nLeft: \n" + left);
            System.out.println("\nJoin Kind: \n" + left.getOperandList().get(0));

            SqlSelect subSql = (SqlSelect) left.getOperandList().get(0);
            System.out.println("\nLeft Sql Column:\n " + subSql.getSelectList().getList());

            SqlIdentifier identifier = (SqlIdentifier) left.getOperandList().get(1);
            System.out.println("\nIdentifier: " + identifier.getSimple());

            node.getOperandList()
                    .forEach(x -> System.out.println("nodeList: " + x));
        }
    }
}
