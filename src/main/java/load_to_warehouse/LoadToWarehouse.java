package load_to_warehouse;

import Email.EmailService;
import database.JDBCUtil;

import java.sql.*;
import java.time.LocalDateTime;

public class LoadToWarehouse {

    public static void loadToWareHouse() {
        try (Connection connection = JDBCUtil.getConnection()) {  // Sử dụng try-with-resources để tự động đóng kết nối
            if (connection == null) {
                System.out.println("Không thể kết nối tới cơ sở dữ liệu.");
                return;
            }

            // 4.7 Thực thi stored procedure "load_to_warehouse_proc"(Trong procedure có sẵn các bước từ 4.1 đến 4.6)
            try (CallableStatement callableStatement = connection.prepareCall("{CALL load_to_warehouse_proc()}")) {
                callableStatement.execute();
                System.out.println("Quá trình load to warehouse đã hoàn thành.");
            } catch (SQLException e) {
                System.err.println("Lỗi khi gọi stored procedure: " + e.getMessage());
                sendEmail("Lỗi trong quá trình load dữ liệu", e.getMessage());
                return;
            }

            // 4.9 Đóng kết nối đã được xử lý tự động bởi try-with-resources
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logError(Connection connection, String fileName, String errorMessage) {
        String procedureCall = "{CALL log_error_proc(?, ?)}"; // Gọi procedure log_error_proc

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            // Set các tham số cho procedure
            callableStatement.setString(1, fileName);
            callableStatement.setString(2, errorMessage);

            // Thực thi stored procedure
            callableStatement.execute();
            System.out.println("Log lỗi đã được ghi: " + errorMessage);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logSuccess(Connection connection, String message) {
        String procedureCall = "{CALL log_success_proc(?, ?)}"; // Gọi procedure log_success_proc

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            // Set các tham số cho procedure
            callableStatement.setString(1, "data.csv"); // Tên tệp
            callableStatement.setString(2, message); // Thông báo log

            // Thực thi stored procedure
            callableStatement.execute();
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
