package test;

import database.JDBCUtil;

import java.sql.Connection;

public class JDBCUtilTest {
    public static void main(String[] args) {
        Connection connection = JDBCUtil.getConnection();
        JDBCUtil.printInfo(connection);
        JDBCUtil.closeConnection(connection);
    }
}
