package test;

import Email.EmailService;
import database.JDBCUtil;

import java.sql.*;
import java.time.LocalDateTime;

public class DataProcessing {

    public static void loadToStaging() {
        // 3.1. Kết nối cơ sở dữ liệu
        try (Connection connection = JDBCUtil.getConnection()) {
            System.out.println("Kết nối cơ sở dữ liệu thành công!");

            // 3.2. Tìm file data mới nhất
            String latestFile = findLatestFile(connection);

            if (latestFile == null) {
                // 3.2.1.1. Ghi log khi không tìm thấy file data
                logError(connection, latestFile, "Không tìm thấy file data");
                sendEmail("Không tìm thấy file data", "Hệ thống không tìm thấy file data mới nhất để load.");
            } else {
                System.out.println("File mới nhất: " + latestFile);
            }

            // 3.3. Kiểm tra procedure và gọi procedure load dữ liệu vào laptop_data_temp
            loadDataProcedure(connection, latestFile);

            // 3.4. Gọi procedure lọc sản phẩm từ bảng laptop_data_temp
            filterProductProcedure(connection, latestFile);

            // 3.4.1. Kiểm tra kết quả sau khi load dữ liệu vào laptop_data_temp
            checkDataLoaded(connection, latestFile);

            // 3.4.2. Kiểm tra dữ liệu rỗng
            handleNullData(connection, latestFile);

            // 3.5. Gọi procedure load dữ liệu vào các bảng dim
            populateDimensions(connection, latestFile);

            // 3.6. Gọi procedure cập nhật lại dữ liệu cho bảng staging_laptop_data
            updateStagingLaptopData(connection, latestFile);

            // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
            logSuccess(connection, "Dữ liệu đã được xử lý thành công và cập nhật vào staging_laptop_data.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try (Connection connection = JDBCUtil.getConnection()) {
            System.out.println("Kết nối cơ sở dữ liệu thành công!");

            // 3.2. Tìm file data mới nhất
            String latestFile = findLatestFile(connection);

            if (latestFile == null) {
                // 3.2.1.1. Ghi log khi không tìm thấy file data
                logError(connection,latestFile, "Không tìm thấy file data");
                sendEmail("Không tìm thấy file data", "Hệ thống không tìm thấy file data mới nhất để load.");
            } else {
                System.out.println("File mới nhất: " + latestFile);
            }

            // 3.3. Kiểm tra procedure và gọi procedure load dữ liệu vào laptop_data_temp
            loadDataProcedure(connection, latestFile);

            // 3.4. Gọi procedure lọc sản phẩm từ bảng laptop_data_temp
            filterProductProcedure(connection, latestFile);

            // 3.4.1. Kiểm tra kết quả sau khi load dữ liệu vào laptop_data_temp
            checkDataLoaded(connection, latestFile);

            // 3.4.2. Kiểm tra dữ liệu rỗng
            handleNullData(connection, latestFile);

            // 3.5. Gọi procedure load dữ liệu vào các bảng dim
            populateDimensions(connection, latestFile);

            // 3.6. Gọi procedure cập nhật lại dữ liệu cho bảng staging_laptop_data
            updateStagingLaptopData(connection, latestFile);

            // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
            logSuccess(connection, "Dữ liệu đã được xử lý thành công và cập nhật vào staging_laptop_data.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 3.2. Tìm file data mới nhất
    private static String findLatestFile(Connection connection) {
        String queryFindLatestFile = """
            SELECT filename 
            FROM log 
            WHERE event = 'crawler data' 
              AND status = 'SU' 
            ORDER BY dt_update DESC 
            LIMIT 1""";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(queryFindLatestFile)) {
            if (resultSet.next()) {
                String filename = resultSet.getString("filename");
                // Cập nhật đường dẫn file với ổ D, sử dụng dấu '/' thay vì '\'
                return "D:/warehouse/data/" + filename; // Sử dụng dấu '/' thay vì '\'
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 3.3. Kiểm tra procedure và gọi procedure load dữ liệu vào laptop_data_temp
    private static void loadDataProcedure(Connection connection, String latestFile) {
        if (latestFile != null && !latestFile.isEmpty()) {
            // 3.3.1. Kiểm tra nếu latestFile không phải null và không rỗng
            System.out.println("Đang load dữ liệu từ file: " + latestFile);

            // Câu lệnh SQL để tải dữ liệu từ file CSV vào bảng laptop_data_temp
            String sql = "LOAD DATA INFILE '" + latestFile + "' INTO TABLE staging_laptop_data " +
                    "FIELDS TERMINATED BY ',' " +      // Cách phân tách các trường dữ liệu
                    "ENCLOSED BY '\"' " +              // Ký tự bao quanh các trường dữ liệu
                    "LINES TERMINATED BY '\\n' " +     // Cách phân tách các dòng dữ liệu
                    "IGNORE 1 ROWS " +                 // Bỏ qua dòng đầu tiên (tiêu đề)
                    "(name, price, trademark, type, status, cpu, ram, hard_drive, " +
                    "screen, graphics_card, operating_system, warranty, import_date, expiration_date);";

            try (Statement statement = connection.createStatement()) {
                // Thực thi câu lệnh SQL
                statement.execute(sql);
                System.out.println("Dữ liệu đã được load thành công từ file: " + latestFile);
            } catch (SQLException e) {
                // 3.3.4. Ghi log khi có lỗi trong quá trình thực thi câu lệnh SQL
                logError(connection, latestFile, "Lỗi khi thực thi câu lệnh LOAD DATA INFILE: " + e.getMessage());
                // 3.3.5. Gửi email khi có lỗi
                sendEmail("Lỗi trong quá trình load dữ liệu", "Có lỗi trong khi tải dữ liệu từ file: " + latestFile + ".\n" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // 3.3.1.1. Ghi log và gửi email khi không tìm thấy đường dẫn file hợp lệ
            System.out.println("Đường dẫn file không hợp lệ: " + latestFile);
            logError(connection, latestFile, "Đường dẫn file không hợp lệ.");
            sendEmail("Lỗi trong quá trình load dữ liệu", "Không tìm thấy đường dẫn file hợp lệ: " + latestFile);
        }
    }


    // 3.4. Gọi procedure lọc sản phẩm từ bảng laptop_data_temp
    private static void filterProductProcedure(Connection connection, String latestFile) {
        String procedureFilterData = getProcedureName(connection, "filter_product_keywords");
        if (procedureFilterData != null) {
            System.out.println("Thực hiện procedure lọc sản phẩm: " + procedureFilterData);
            try (CallableStatement filterStatement = connection.prepareCall("{CALL " + procedureFilterData + "}")) {
                filterStatement.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            // 3.4.1. Ghi log khi không tìm thấy procedure lọc sản phẩm
            logError(connection, latestFile, "Không tìm thấy procedure lọc sản phẩm");
            sendEmail("Lỗi trong quá trình lọc sản phẩm", "Không tìm thấy procedure lọc sản phẩm.");
        }
    }

    // 3.4.1. Kiểm tra kết quả sau khi load dữ liệu vào laptop_data_temp
    private static void checkDataLoaded(Connection connection, String latestFile) {
        if (!checkDataLoaded(connection)) {
            logError(connection, latestFile, "Không thể lọc ra sản phẩm tại dòng/cột...");
        }
    }

    // 3.4.2. Kiểm tra dữ liệu rỗng
    private static void handleNullData(Connection connection, String latestFile) {
        if (isDataEmpty(connection)) {
            // 3.4.2.1. Gọi procedure xử lý dữ liệu trống (null)
            String procedureHandleNull = getProcedureName(connection, "handle_null_data");
            if (procedureHandleNull != null) {
                System.out.println("Thực hiện procedure xử lý null: " + procedureHandleNull);
                try (CallableStatement handleNullStatement = connection.prepareCall("{CALL " + procedureHandleNull + "}")) {
                    handleNullStatement.execute();

                    // 3.4.2.2. Kiểm tra sau khi xử lý null
                    if (!isNullHandlingSuccessful(connection)) {
                        logError(connection, latestFile, "Lỗi khi xử lý dữ liệu null");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            // Nếu không có dữ liệu trống (null), tiếp tục vào bước 3.5
            System.out.println("Không có dữ liệu trống (null), tiếp tục vào bước 3.5.");
        }
    }

    // 3.5. Gọi procedure load dữ liệu vào các bảng dim
    private static void populateDimensions(Connection connection, String latestFile) {
        String procedurePopulateDimensions = getProcedureName(connection, "populate_dimensions");
        if (procedurePopulateDimensions != null) {
            try (CallableStatement populateDimStatement = connection.prepareCall("{CALL " + procedurePopulateDimensions + "}")) {
                populateDimStatement.execute();
                System.out.println("Dữ liệu đã được load vào bảng dim thành công.");
                if (!checkDataLoaded(connection)) {
                    // 3.5.1.1. Ghi log nếu load dữ liệu không thành công
                    logError(connection, latestFile, "Không thể load dữ liệu vào bảng dim.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            logError(connection, latestFile, "Không tìm thấy procedure populate_dimensions");
            sendEmail("Lỗi trong quá trình load dữ liệu vào bảng dim", "Không tìm thấy procedure populate_dimensions.");
        }
    }

    // 3.6. Gọi procedure cập nhật lại dữ liệu cho bảng staging_laptop_data
    private static void updateStagingLaptopData(Connection connection, String latestFile) {
        String procedureUpdateStaging = getProcedureName(connection, "update_staging_laptop_data");
        if (procedureUpdateStaging != null) {
            try (CallableStatement updateStagingStatement = connection.prepareCall("{CALL " + procedureUpdateStaging + "}")) {
                updateStagingStatement.execute();
                System.out.println("Dữ liệu đã được cập nhật cho bảng staging_laptop_data.");
                if (!checkDataLoaded(connection)) {
                    logError(connection, latestFile, "Cập nhật dữ liệu vào bảng staging_laptop_data không thành công.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            logError(connection, latestFile, "Không tìm thấy procedure update_staging_laptop_data");
            sendEmail("Lỗi trong quá trình cập nhật dữ liệu bảng staging_laptop_data", "Không tìm thấy procedure update_staging_laptop_data.");
        }
    }

    // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
    private static void logSuccess(Connection connection, String message) {
        String logInsertQuery = """
                INSERT INTO log(id_config, filename, date, event, status, file_size, dt_update, error_message)
                VALUES (?, ?, ?, 'process', 'SU', ?, ?, ?)""" ;

        try (PreparedStatement preparedStatement = connection.prepareStatement(logInsertQuery)) {
            preparedStatement.setInt(1, 19);
            preparedStatement.setString(2, "data.csv");
            preparedStatement.setDate(3, new java.sql.Date(System.currentTimeMillis()));
            preparedStatement.setLong(4, 0); // Kích thước file giả định
            preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(6, message);
            preparedStatement.executeUpdate();
            System.out.println("Log thành công: " + message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void logError(Connection connection, String fileName, String errorMessage) {
        String logInsertQuery = """
                INSERT INTO log(id_config, filename, date, event, status, file_size, dt_update, error_message)
                VALUES (?, ?, ?, 'load to staging', 'EF', ?, ?, ?)""" ;

        try (PreparedStatement preparedStatement = connection.prepareStatement(logInsertQuery)) {
            preparedStatement.setInt(1, 19); // ID_config giả định là 1
            preparedStatement.setString(2, fileName);
            preparedStatement.setDate(3, new java.sql.Date(System.currentTimeMillis()));
            preparedStatement.setLong(4, 0); // Kích thước file giả định
            preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setString(6, errorMessage);
            preparedStatement.executeUpdate();
            System.out.println("Log lỗi đã được ghi: " + errorMessage);
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
        String query = "SELECT COUNT(*) AS total FROM staging_laptop_data";
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
        // Lấy danh sách các cột trong bảng staging_laptop_data
        String getColumnsQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'staging_laptop_data'";

        // Chuỗi chứa các điều kiện để kiểm tra null cho tất cả các cột
        StringBuilder columnChecks = new StringBuilder();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(getColumnsQuery)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnChecks.length() > 0) {
                    columnChecks.append(" OR ");
                }
                columnChecks.append(columnName).append(" IS NULL");
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false; // Trong trường hợp có lỗi, giả sử không có dữ liệu rỗng
        }

        // Kiểm tra nếu không có cột nào, tức là bảng trống hoặc không có cột
        if (columnChecks.length() == 0) {
            return false;
        }

        // Truy vấn kiểm tra dữ liệu null
        String query = "SELECT COUNT(*) AS total FROM staging_laptop_data WHERE " + columnChecks.toString();
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