package load_to_warehouse;

import Email.EmailService;
import database.JDBCUtil;

import java.sql.*;
import java.time.LocalDateTime;

public class LoadToWarehouse {

    public static void loadToWareHouse() {
        try {
            // 4.1 kết nối db
            Connection connection = JDBCUtil.getConnection();
            if (connection == null) {
                System.out.println("Không thể kết nối tới cơ sở dữ liệu.");
                return;
            }

            // 4.2 Kiểm tra trạng thái "Transform field"
            String transformCheckQuery = "SELECT * FROM log WHERE event = 'Transform field' AND DATE(dt_update) = CURDATE() AND status = 'SU'";
            try (PreparedStatement transformCheckStmt = connection.prepareStatement(transformCheckQuery);
                 ResultSet transformCheckResult = transformCheckStmt.executeQuery()) {

                if (!transformCheckResult.next()) {
                    System.out.println("Chưa thực hiện transform field. Dừng quá trình.");
                    return;
                }
            }

            // 4.3 Kiểm tra trạng thái "Load to Data Warehouse"
            String loadCheckQuery = "SELECT * FROM log WHERE event = 'load to data warehouse' AND DATE(dt_update) = CURDATE() AND status = 'SU'";
            try (PreparedStatement loadCheckStmt = connection.prepareStatement(loadCheckQuery);
                 ResultSet loadCheckResult = loadCheckStmt.executeQuery()) {

                if (loadCheckResult.next()) {
                    System.out.println("Đã thực hiện load to warehouse.");
                    return;
                }
            }

            // 4.4 Ghi log "bắt đầu thực hiện load to warehouse"
            String startLogQuery = "INSERT INTO log(event, status, dt_update) VALUES ('load to data warehouse', 'IP', ?)";
            try (PreparedStatement startLogStmt = connection.prepareStatement(startLogQuery)) {
                startLogStmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                startLogStmt.executeUpdate();
                System.out.println("Bắt đầu thực hiện load to warehouse.");
            }

            // 4.5

            // 4.6

            // 4.7 Thực thi procedure "load_to_warehouse"
            String procedureName = getProcedureName(connection, "load_to_warehouse");
            if (procedureName != null) {
                System.out.println("Thực hiện procedure: " + procedureName);
                try (CallableStatement callableStatement = connection.prepareCall("{CALL " + procedureName + "}")) {
                    callableStatement.execute();
                }

                // 4.8 Ghi log "load to data warehouse thành công"
                String successLogQuery = "INSERT INTO log(event, status, dt_update) VALUES ('load to data warehouse', 'SU', ?)";
                try (PreparedStatement successLogStmt = connection.prepareStatement(successLogQuery)) {
                    successLogStmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                    successLogStmt.executeUpdate();
                }
                System.out.println("Load to warehouse thành công.");
            } else {
                System.err.println("Không tìm thấy procedure load_to_warehouse.");
                sendEmail("Lỗi trong quá trình load dữ liệu", "Không tìm thấy procedure load dữ liệu.");
                return;
            }

            // 4.9 Đóng kết nối
            JDBCUtil.closeConnection(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logError(Connection connection, String fileName, String errorMessage) {
        String logInsertQuery = """
                INSERT INTO log(filename, date, event, status, file_size, dt_update, error_message)
                VALUES (?, ?, 'load to staging', 'EF', ?, ?, ?)""";

        try (PreparedStatement preparedStatement = connection.prepareStatement(logInsertQuery)) {
            preparedStatement.setString(1, fileName);
            preparedStatement.setDate(2, new java.sql.Date(System.currentTimeMillis()));
            preparedStatement.setLong(3, 0); // Kích thước file giả định
            preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(5, errorMessage);
            preparedStatement.executeUpdate();
            System.out.println("Log lỗi đã được ghi: " + errorMessage);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logSuccess(Connection connection, String message) {
        String logInsertQuery = """
                INSERT INTO log(filename, date, event, status, file_size, dt_update, error_message)
                VALUES (?, ?, 'process', 'SU', ?, ?, ?)""";

        try (PreparedStatement preparedStatement = connection.prepareStatement(logInsertQuery)) {
            preparedStatement.setString(1, "data.csv");
            preparedStatement.setDate(2, new java.sql.Date(System.currentTimeMillis()));
            preparedStatement.setLong(3, 0); // Kích thước file giả định
            preparedStatement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(5, message);
            preparedStatement.executeUpdate();
            System.out.println("Log thành công: " + message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void sendEmail(String subject, String messageContent) {
        EmailService emailService = new EmailService();
        String to = "21130551@st.hcmuaf.edu.vn";
        if (emailService.send(to, subject, messageContent)) {
            System.out.println("Email thông báo đã được gửi.");
        } else {
            System.err.println("Gửi email thất bại.");
        }
    }

    private static String getProcedureName(Connection connection, String name) {
        String query = "SELECT procedure_name FROM control WHERE name = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, name);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("procedure_name");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
