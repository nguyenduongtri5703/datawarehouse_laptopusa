package database;


import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {

    public static Connection getConnection() {
        Connection c = null;

        try {
            // Đăng ký MySQL Driver với DriverManager
            DriverManager.registerDriver(new Driver());

            // Các thông số
            String url = "jdbc:mySQL://" + DBProperties.host + ":" + DBProperties.port + "/" + DBProperties.dbName;
            String username = DBProperties.username;
            String password = DBProperties.password;

            // Tạo kết nối
            c = DriverManager.getConnection(url, username, password);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return c;
    }

    public static void closeConnection(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printInfo(Connection c){
        try {
            if (c != null){
                DatabaseMetaData mtdt = c.getMetaData();
                System.out.println(mtdt.getDatabaseProductName());
                System.out.println(mtdt.getDatabaseProductVersion());
            }
        } catch (SQLException e){
            e.printStackTrace();
        }
    }
}
