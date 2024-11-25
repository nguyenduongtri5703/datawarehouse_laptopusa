package test;

import Email.EmailService;
import database.JDBCUtil;

import java.sql.*;
import java.time.LocalDateTime;

public class DataProcessing {

    public static void main(String[] args) {
        try (Connection connection = JDBCUtil.getConnection()) {
            System.out.println("Kết nối cơ sở dữ liệu thành công!");

            // 3.2. Tìm file data mới nhất
            String queryFindLatestFile = """
                    SELECT file_name 
                    FROM log 
                    WHERE event = 'crawler data' 
                      AND status = 'SU' 
                    ORDER BY dt_update DESC 
                    LIMIT 1""";

            String latestFile = null;
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(queryFindLatestFile)) {

                if (resultSet.next()) {
                    latestFile = resultSet.getString("file_name");
                    System.out.println("File mới nhất: " + latestFile);
                } else {
                    // 3.2.1.1. Ghi log khi không tìm thấy file data
                    logError(connection, "data.csv", "Không tìm thấy file data");
                    sendEmail("Không tìm thấy file data", "Hệ thống không tìm thấy file data mới nhất để load.");
                }
            }

            // 3.3. Kiểm tra procedure và gọi procedure load dữ liệu vào laptop_data_temp
            String procedureLoadData = getProcedureName(connection, "load_data");
            if (procedureLoadData != null) {
                // 3.3.1. Kiểm tra procedure và thực hiện load dữ liệu từ file csv vào laptop_data_temp
                System.out.println("Thực hiện procedure load dữ liệu: " + procedureLoadData);
                CallableStatement callableStatement = connection.prepareCall("{CALL " + procedureLoadData + "}");
                callableStatement.execute();
            } else {
                // 3.3.1.1. Ghi log khi không tìm thấy procedure load dữ liệu
                logError(connection, latestFile, "Không tìm thấy procedure load dữ liệu");
                sendEmail("Lỗi trong quá trình load dữ liệu", "Không tìm thấy procedure load dữ liệu.");
            }

            // 3.4. Gọi procedure lọc sản phẩm từ bảng laptop_data_temp
            String procedureFilterData = getProcedureName(connection, "filter_product_keywords");
            if (procedureFilterData != null) {
                System.out.println("Thực hiện procedure lọc sản phẩm: " + procedureFilterData);
                CallableStatement filterStatement = connection.prepareCall("{CALL " + procedureFilterData + "}");
                filterStatement.execute();
            } else {
                // 3.4.1. Ghi log khi không tìm thấy procedure lọc sản phẩm
                logError(connection, latestFile, "Không tìm thấy procedure lọc sản phẩm");
                sendEmail("Lỗi trong quá trình lọc sản phẩm", "Không tìm thấy procedure lọc sản phẩm.");
            }

            // 3.4.1. Kiểm tra kết quả sau khi load dữ liệu vào laptop_data_temp
            if (!checkDataLoaded(connection)) {
                logError(connection, latestFile, "Không thể lọc ra sản phẩm tại dòng/cột...");
            }

            // 3.4.2. Kiểm tra dữ liệu rỗng
            if (isDataEmpty(connection)) {
                // 3.4.2.1. Gọi procedure xử lý dữ liệu trống (null)
                String procedureHandleNull = getProcedureName(connection, "handle_null_data");
                if (procedureHandleNull != null) {
                    System.out.println("Thực hiện procedure xử lý null: " + procedureHandleNull);
                    CallableStatement handleNullStatement = connection.prepareCall("{CALL " + procedureHandleNull + "}");
                    handleNullStatement.execute();

                    // 3.4.2.2. Kiểm tra sau khi xử lý null
                    if (!isNullHandlingSuccessful(connection)) {
                        logError(connection, latestFile, "Lỗi khi xử lý dữ liệu null");
                    }
                }
            } else {
                // Nếu không có dữ liệu trống (null), tiếp tục vào bước 3.5
                System.out.println("Không có dữ liệu trống (null), tiếp tục vào bước 3.5.");
            }

            // 3.5. Gọi procedure load dữ liệu vào các bảng dim
            String procedurePopulateDimensions = getProcedureName(connection, "populate_dimensions");
            if (procedurePopulateDimensions != null) {
                CallableStatement populateDimStatement = connection.prepareCall("{CALL " + procedurePopulateDimensions + "}");
                populateDimStatement.execute();
                System.out.println("Dữ liệu đã được load vào bảng dim thành công.");

                // 3.5.1. Kiểm tra kết quả sau khi load dữ liệu vào bảng dim
                if (!checkDataLoaded(connection)) {
                    // 3.5.1.1. Ghi log nếu load dữ liệu không thành công
                    logError(connection, latestFile, "Không thể load dữ liệu vào bảng dim.");
                }

                // 3.6. Gọi procedure cập nhật lại dữ liệu cho bảng staging_laptop_data
                String procedureUpdateStaging = getProcedureName(connection, "update_staging_laptop_data");
                if (procedureUpdateStaging != null) {
                    CallableStatement updateStagingStatement = connection.prepareCall("{CALL " + procedureUpdateStaging + "}");
                    updateStagingStatement.execute();
                    System.out.println("Dữ liệu đã được cập nhật cho bảng staging_laptop_data.");

                    // 3.6.1. Kiểm tra kết quả sau khi cập nhật dữ liệu cho bảng staging_laptop_data
                    if (!checkDataLoaded(connection)) {
                        // 3.6.1.1. Ghi log nếu cập nhật dữ liệu không thành công
                        logError(connection, latestFile, "Cập nhật dữ liệu vào bảng staging_laptop_data không thành công.");
                    }

                    // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
                    logSuccess(connection, "Dữ liệu đã được xử lý thành công và cập nhật vào staging_laptop_data.");
                }
            } else {
                logError(connection, latestFile, "Không tìm thấy procedure populate_dimensions");
                sendEmail("Lỗi trong quá trình load dữ liệu vào bảng dim", "Không tìm thấy procedure populate_dimensions.");
            }
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

    private static boolean checkDataLoaded(Connection connection) {
        // Giả sử ta kiểm tra số lượng dòng trong `laptop_data_temp` lớn hơn 0
        String query = "SELECT COUNT(*) AS total FROM laptop_data_temp";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getInt("total") > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean isDataEmpty(Connection connection) {
        // Giả sử kiểm tra xem có dòng nào có dữ liệu null không
        String query = "SELECT COUNT(*) AS total FROM laptop_data_temp WHERE column_name IS NULL";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getInt("total") > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean isNullHandlingSuccessful(Connection connection) {
        // Kiểm tra lại xem sau xử lý có còn null không
        return !isDataEmpty(connection);
    }
}
