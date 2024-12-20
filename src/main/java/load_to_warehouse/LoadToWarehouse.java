package load_to_warehouse;

import Email.EmailService;
import database.JDBCUtil;

import java.sql.*;
import java.time.LocalDateTime;

public class LoadToWarehouse {

    public static void loadToWareHouse() {

        // 4.1 Kết nối DB
        try (Connection connection = JDBCUtil.getConnection()) { // Sử dụng try-with-resources để tự động đóng kết nối
            if (connection == null) {
                System.out.println("Không thể kết nối tới cơ sở dữ liệu.");
                return;
            }

            // 4.2 Kiểm tra trạng thái "Transform field"
            String transformCheckProc = getProcedureName(connection, "check_transform_field");
            if (transformCheckProc != null) {
                try (CallableStatement stmt = connection.prepareCall("{CALL " + transformCheckProc + "()}")) {
                    ResultSet rs = stmt.executeQuery();
                    if (!rs.next() || rs.getInt(1) == 0) { // Giả định stored procedure trả về một giá trị
                        System.out.println("Chưa thực hiện transform field. Dừng quá trình.");
                        return;
                    }
                }
            } else {
                System.out.println("Không tìm thấy procedure check_transform_field.");
                return;
            }

            // 4.3 Kiểm tra trạng thái "Load to Data Warehouse"
            String loadCheckProc = getProcedureName(connection, "check_load_to_dw");
            if (loadCheckProc != null) {
                try (CallableStatement stmt = connection.prepareCall("{CALL " + loadCheckProc + "()}")) {
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next() && rs.getInt(1) > 0) { // Giả định stored procedure trả về một giá trị
                        System.out.println("Đã thực hiện load to warehouse.");
                        return;
                    }
                }
            } else {
                System.out.println("Không tìm thấy procedure check_load_to_dw.");
                return;
            }

            // 4.4 Ghi log "bắt đầu thực hiện load to warehouse"
            String logStartProc = getProcedureName(connection, "log_event");
            if (logStartProc != null) {
                try (CallableStatement stmt = connection.prepareCall("{CALL " + logStartProc + "(?, ?, ?)}")) {
                    stmt.setString(1, "load to data warehouse");
                    stmt.setString(2, "IP");
                    stmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                    stmt.execute();
                    System.out.println("Bắt đầu thực hiện load to warehouse.");
                }
            } else {
                System.out.println("Không tìm thấy procedure log_event.");
                return;
            }

            // 4.5
            // Lấy tên stored procedure từ bảng control
            String procedureName = getProcedureName(connection, "load_to_dw");

            if (procedureName != null) {
                // Gọi stored procedure lấy được từ control
                try (CallableStatement callableStatement = connection.prepareCall("{CALL " + procedureName + "()}")) {
                    callableStatement.execute();
                    System.out.println("Quá trình load to warehouse đã hoàn thành.");
                    // 4.6
                    // Ghi log thành công
                    logSuccess(connection, "Quá trình load dữ liệu thành công.");
                } catch (SQLException e) {
                    System.err.println("Lỗi khi gọi stored procedure: " + e.getMessage());
                    // Ghi log lỗi
                    logError(connection, "data.csv", e.getMessage());
                    sendEmail("Lỗi trong quá trình load dữ liệu", e.getMessage());
                }
            } else {
                // Xử lý nếu không tìm thấy tên stored procedure
                System.err.println("Không tìm thấy procedure load_to_warehouse_proc trong bảng control.");
                // Ghi log lỗi
                logError(connection, "data.csv", "Không tìm thấy procedure load_to_dw.");
                sendEmail("Lỗi trong quá trình load dữ liệu", "Không tìm thấy procedure load_to_warehouse_proc.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logError(Connection connection, String fileName, String errorMessage) {
        // Lấy tên stored procedure từ bảng control
        String procedureName = getProcedureName(connection, "log_error_proc");

        if (procedureName != null) {
            String procedureCall = "{CALL " + procedureName + "(?, ?)}"; // Câu lệnh CALL với tên procedure động

            try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
                // Set các tham số cho procedure
                callableStatement.setString(1, fileName);
                callableStatement.setString(2, errorMessage);

                // Thực thi stored procedure
                callableStatement.execute();
                System.out.println("Log lỗi đã được ghi: " + errorMessage);
            } catch (SQLException e) {
                System.err.println("Lỗi khi gọi stored procedure: " + e.getMessage());
            }
        } else {
            // Xử lý nếu không tìm thấy tên procedure
            System.err.println("Không tìm thấy procedure log_error_proc trong bảng control.");
            sendEmail("Lỗi khi ghi log lỗi", "Không tìm thấy procedure log_error_proc.");
        }
    }

    private static void logSuccess(Connection connection, String message) {
        // Lấy tên stored procedure từ bảng control
        String procedureName = getProcedureName(connection, "log_success_proc");

        if (procedureName != null) {
            String procedureCall = "{CALL " + procedureName + "(?, ?)}"; // Câu lệnh CALL với tên procedure động

            try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
                // Set các tham số cho procedure
                callableStatement.setString(1, "data.csv"); // Tên tệp
                callableStatement.setString(2, message);   // Thông báo log

                // Thực thi stored procedure
                callableStatement.execute();
                System.out.println("Log thành công: " + message);
            } catch (SQLException e) {
                System.err.println("Lỗi khi gọi stored procedure: " + e.getMessage());
            }
        } else {
            // Xử lý nếu không tìm thấy tên procedure
            System.err.println("Không tìm thấy procedure log_success_proc trong bảng control.");
            sendEmail("Lỗi khi ghi log thành công", "Không tìm thấy procedure log_success_proc.");
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
