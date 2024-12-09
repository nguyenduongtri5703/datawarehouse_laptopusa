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
                logError(connection, latestFile, "load to staging","Không tìm thấy file data");
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

            // 3.7. Ghi log khi hoàn thành toàn bộ quá trình transform field thành công
            logFieldSuccess(connection, "transform field ","transform field success");

            // 3.8. Ghi log khi hoàn thành toàn bộ quá trình load to staging thành công
            logStagingSuccess(connection, "load to staging","load to staging success");

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
                logError(connection, latestFile, "load to staging","Không tìm thấy file data");
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

            // 3.7. Ghi log khi hoàn thành toàn bộ quá trình transform field thành công
            logFieldSuccess(connection, "transform field ","transform field success");

            // 3.8. Ghi log khi hoàn thành toàn bộ quá trình load to staging thành công
            logStagingSuccess(connection, "load to staging","load to staging success");
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
                    "screen, graphics_card, operating_system, warranty, import_date, @expiration_date) " + // Sử dụng biến @expiration_date
                    "SET expiration_date = '9999-12-31';"; // Gán giá trị mặc định cho expiration_date

            try (Statement statement = connection.createStatement()) {
                // Thực thi câu lệnh SQL
                statement.execute(sql);
                System.out.println("Dữ liệu đã được load thành công từ file: " + latestFile);
            } catch (SQLException e) {
                logError(connection, latestFile, "load to staging","Lỗi khi thực thi câu lệnh LOAD DATA INFILE: " + e.getMessage());
                sendEmail("Lỗi trong quá trình load dữ liệu", "Có lỗi trong khi tải dữ liệu từ file: " + latestFile + ".\n" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // 3.3.1.1. Ghi log và gửi email khi không tìm thấy đường dẫn file hợp lệ
            System.out.println("Đường dẫn file không hợp lệ: " + latestFile);
            logError(connection, latestFile,"load to staging","Đường dẫn file không hợp lệ.");
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
            // 3.4.1 Ghi log khi không tìm thấy procedure lọc sản phẩm
            logError(connection, latestFile, "load to staging","Không tìm thấy procedure lọc sản phẩm");
            sendEmail("Lỗi trong quá trình lọc sản phẩm", "Không tìm thấy procedure lọc sản phẩm.");
        }
    }

    // 3.4.1.1 Kiểm tra kết quả sau khi load dữ liệu vào laptop_data_temp
    private static void checkDataLoaded(Connection connection, String latestFile) {
        if (!checkDataLoaded(connection)) {
            logError(connection, latestFile,"transform field","Không thể lọc ra sản phẩm tại dòng/cột...");
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
                        logError(connection, latestFile, "transform field","Lỗi khi xử lý dữ liệu null");
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
                    logError(connection, latestFile, "transform field","Không thể load dữ liệu vào bảng dim.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            logError(connection, latestFile, "transform field","Không tìm thấy procedure populate_dimensions");
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
                    logError(connection, latestFile, "transform field","Cập nhật dữ liệu vào bảng staging_laptop_data không thành công.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            logError(connection, latestFile, "transform field","Không tìm thấy procedure update_staging_laptop_data");
            sendEmail("Lỗi trong quá trình cập nhật dữ liệu bảng staging_laptop_data", "Không tìm thấy procedure update_staging_laptop_data.");
        }
    }

    // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
    // Gọi stored procedure log_process_success
    public static void logFieldSuccess(Connection connection,String event, String message) {
        String storedProc = "{CALL log_process_success(?, ?, ?, ?)}"; // Gọi stored procedure

        try (CallableStatement callableStatement = connection.prepareCall(storedProc)) {
            callableStatement.setInt(1, 19); // Tham số id_config
            callableStatement.setString(2, "data.csv"); // Tham số filename
            callableStatement.setString(3, event);
            callableStatement.setString(4, message); // Tham số message

            // Thực thi stored procedure
            callableStatement.execute();
            System.out.println("Log thành công: " + message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // 3.7. Ghi log khi hoàn thành toàn bộ quá trình thành công
    // Gọi stored procedure log_process_success
    public static void logStagingSuccess(Connection connection,String event, String message) {
        String storedProc = "{CALL log_process_success(?, ?, ?, ?)}"; // Gọi stored procedure

        try (CallableStatement callableStatement = connection.prepareCall(storedProc)) {
            callableStatement.setInt(1, 19); // Tham số id_config
            callableStatement.setString(2, "data.csv"); // Tham số filename
            callableStatement.setString(3, event);
            callableStatement.setString(4, message); // Tham số message

            // Thực thi stored procedure
            callableStatement.execute();
            System.out.println("Log thành công: " + message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Gọi stored procedure log_error
    public static void logError(Connection connection, String fileName ,String event ,String errorMessage) {
        String storedProc = "{CALL log_error(?, ?, ?, ?)}"; // Gọi stored procedure

        try (CallableStatement callableStatement = connection.prepareCall(storedProc)) {
            callableStatement.setInt(1, 19); // Tham số id_config
            callableStatement.setString(2, fileName); // Tham số filename
            callableStatement.setString(3, event); // Tham số filename
            callableStatement.setString(4, errorMessage); // Tham số error_message

            // Thực thi stored procedure
            callableStatement.execute();
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

    // Gọi stored procedure check_data_loaded
    public static boolean checkDataLoaded(Connection connection) {
        String storedProc = "{CALL check_data_loaded(?)}"; // Gọi stored procedure

        try (CallableStatement callableStatement = connection.prepareCall(storedProc)) {
            callableStatement.registerOutParameter(1, Types.BOOLEAN); // Đăng ký tham số OUT

            // Thực thi stored procedure
            callableStatement.execute();

            // Lấy giá trị tham số OUT
            return callableStatement.getBoolean(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean isDataEmpty(Connection connection) {
        // Gọi Stored Procedure trong MySQL
        String procedureCall = "{CALL check_if_data_empty()}";

        try (CallableStatement stmt = connection.prepareCall(procedureCall);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                int total = rs.getInt("total");
                return total > 0;
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