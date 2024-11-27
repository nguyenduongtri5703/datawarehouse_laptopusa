package crawl;

import Email.EmailService;
import Email.IJavaMail;
import database.JDBCUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CrawlProduct {

    // 1. Khởi tạo các biến trong bảng config
    private static String sourceUrl; // Source url từ config
    private static String exportLocation; // Địa chỉ để lưu file từ config
    private static String email; // Email từ config
    private static final String BASE_URL = "https://laptopusa.vn";
    // Tên file
    private static String fileName;
    private static String fileName_2;
    // Đếm số dòng của file
    private static int count ;
    // Kích thước file
    private static double fileSizeKB;
    // Khởi tạo EmailService
    private static final IJavaMail emailService = new EmailService();

    public static void main(String[] args) {
    }
        public static void runCrawl() {

            System.out.println("Loading configuration from database...");

        // 2. Tải/lấy cấu hình từ config trong database
        if (!loadConfigFromDatabase()) {
            System.err.println("Failed to load configuration.");
            return;
        }
        System.out.println("Starting the scraping process...\n");
        List<Map<String, String>> allProducts = new ArrayList<>();
        int page = 1;

        // 3. Duyệt qua các liên kết trong trang laptopusa.vn
        while (true) {
            String collectionUrl = sourceUrl + "?page=" + page;
            System.out.println("Fetching product links from " + collectionUrl + "...");

            // 4. Lấy ra danh sách liên kết của các sản phẩm ( page )
            List<String> productLinks = getProductLinks(collectionUrl);
            // Nếu không có thì break
            if (productLinks.isEmpty()) {
                System.out.println("No more products found on page " + page + ". Stopping.");
//                sendErrorEmail("No products found on page " + page);
//                logErrorToYAML("Không tìm thấy dữ liệu của sản phẩm");

                break;
            }

            // 5. Duyệt vòng lặp qua danh sách liên kết, lấy dữ liệu các sản phẩm
            for (String link : productLinks) {
                Map<String, String> productDetails = scrapeProductDetails(link);
                if (productDetails != null) {
                    allProducts.add(productDetails);
                }
            }
            // Nếu không có dữ liệu sản phẩm thì (6) gửi thông báo lỗi qua email, ghi log error và break
            // Nếu có dữ liệu sản phẩm thì tiếp tục
            if (allProducts.isEmpty()) {
                System.out.println("No product data found from the links. Sending error email...");
                sendErrorEmail("No product data found on page " + page);
                logErrorToDatabase("Không tìm thấy dữ liệu của sản phẩm");

                break;
            }
            page++;

        }

        // Xuất File ( bao gồm 7. thêm dữ liệu vào file và 8. xuất ra file csv )
        exportToCSV(allProducts);
        // 9. Gửi email thông báo xuất file thành công
        sendSuccessEmail();
        // 10. Ghi log sau khi export CSV thành công
        logSuccessToDatabase(fileName_2, count, fileSizeKB);
        System.out.println("\nScraping completed.");
    }

    // Tải cấu hình từ file config
    // Lấy địa chỉ lưu file trong config và gán cho exportLocation
    private static boolean loadConfigFromDatabase() {
        Connection conn = null;

        try {
            conn = JDBCUtil.getConnection();
            Statement stmt = conn.createStatement();

            // Thực thi câu truy vấn để lấy dữ liệu cấu hình ( từ id 19 )
            ResultSet rs = stmt.executeQuery("SELECT source, source_location FROM control WHERE id = '19'");

            if (rs.next()) {
                // Gán các giá trị từ kết quả truy vấn
                // Đường dẫn source
                sourceUrl = rs.getString("source");
                // Địa chỉ lưu file csv
                exportLocation = rs.getString("source_location");

                // Tạo thư mục xuất nếu nó chưa tồn tại
                new File(exportLocation).mkdirs();
                return true;
            } else {
                System.err.println("Không tìm thấy dữ liệu cấu hình trong cơ sở dữ liệu.");
            }
        } catch (Exception e) {
            System.err.println("Lỗi khi tải cấu hình từ cơ sở dữ liệu: " + e.getMessage());
        } finally {
            // Đóng kết nối đến cơ sở dữ liệu
            JDBCUtil.closeConnection(conn);
        }
        return false;
    }


    // (4) Phương thức lấy ra danh sách liên kết của các sản phẩm
    private static List<String> getProductLinks(String url) {
        List<String> productLinks = new ArrayList<>();
        try {
            Document doc = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0")
                    .timeout(10000)
                    .get();
            System.out.println("Successfully fetched the product list from " + url + ".");
            Elements productDivs = doc.select("div.product-img");

            for (Element div : productDivs) {
                Element linkTag = div.selectFirst("a[href]");
                if (linkTag != null) {
                    String productLink = BASE_URL + linkTag.attr("href");
                    productLinks.add(productLink);
                    System.out.println("Found product link: " + productLink);
                }
            }
        } catch (IOException e) {
            System.err.println("Error fetching product links: " + e.getMessage());
        }
        return productLinks;
    }

    // (5)  Phương thức Lấy dữ liệu chi tiết của sản phẩm
    private static Map<String, String> scrapeProductDetails(String url) {
        // Tạo một map để lưu trữ chi tiết sản phẩm với thứ tự bảo toàn
        Map<String, String> productDetails = new LinkedHashMap<>();
        try {
            // Kết nối tới URL sản phẩm, cài đặt user agent và thời gian chờ
            Document doc = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0")
                    .timeout(10000)
                    .get();
            System.out.println("Successfully fetched the product details from " + url + ".");

            // Lấy tên sản phẩm
            Element nameTag = doc.selectFirst("h1[itemprop=name]");
            String productName = nameTag != null ? nameTag.text().trim() : "Not Found";
            productDetails.put("Tên sản phẩm", productName);

            // Lấy giá sản phẩm
            Element priceTag = doc.selectFirst("span.current-price.ProductPrice");
            String price = priceTag != null ? priceTag.text().trim() : "Not Found";
            productDetails.put("Giá", price);

            // Lấy thương hiệu sản phẩm
            Element brandTag = doc.selectFirst("div.pro-brand");
            String brand = brandTag != null ? brandTag.text().replaceFirst("(?i)Thương hiệu\\s*:", "").trim() : "Not Found";
            productDetails.put("Thương hiệu", brand);

            // Lấy loại sản phẩm
            Element typeTag = doc.selectFirst("div.pro-type");
            String type = typeTag != null ? typeTag.text().replaceFirst("(?i)Loại\\s*:", "").trim() : "Not Found";
            productDetails.put("Loại", type);

            // Lấy tình trạng sản phẩm
            Element stockTag = doc.selectFirst("div.pro-stock");
            String stock = stockTag != null ? stockTag.text().replaceFirst("(?i)Tình trạng\\s*:", "").trim() : "Not Found";
            productDetails.put("Tình trạng", stock);

            // Lấy các mô tả sản phẩm, bỏ qua thông tin "Màu sắc"
            Element descTag = doc.selectFirst("div.pro-short-desc");
            List<String> descriptions = new ArrayList<>();
            if (descTag != null) {
                Elements liItems = descTag.select("li");
                for (Element li : liItems) {
                    Element strongTag = li.selectFirst("strong");
                    if (strongTag != null && strongTag.text().contains("Màu sắc")) {
                        continue; // Bỏ qua nếu là "Màu sắc"
                    }
                    descriptions.add(li.text().trim());
                }
            }

            // Ánh xạ các mô tả với các trường cụ thể như CPU, RAM, v.v.
            String[] headers = {"CPU", "RAM", "Đĩa cứng", "Màn hình", "Card đồ họa", "Hệ điều hành", "Bảo hành"};
            for (int i = 0; i < headers.length; i++) {
                if (i < descriptions.size()) {
                    // Loại bỏ tiền tố (như "CPU:", "RAM:") và dấu hai chấm, không phân biệt chữ hoa/thường
                    String description = descriptions.get(i)
                            .replaceFirst("(?i)^" + headers[i] + "\\s*:\\s*", "")
                            .trim();
                    productDetails.put(headers[i], description);
                } else {
                    productDetails.put(headers[i], ""); // Để trống nếu không có mô tả
                }
            }

            // Thêm ngày nhập và ngày hết hạn (để trống)
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String currentDateTime = LocalDateTime.now().format(dtf);
            productDetails.put("Ngày nhập", currentDateTime);
            productDetails.put("Ngày hết hạn", "");

        } catch (IOException e) {
            // Xử lý lỗi nếu không thể lấy dữ liệu từ URL
            System.err.println("Error fetching product details from " + url + ": " + e.getMessage());
            return null;
        }
        return productDetails;
    }

    // (7) Thêm dữ liệu vào file
    // (8) Xuất dữ liệu ra file CSV
    private static void exportToCSV(List<Map<String, String>> products) {
        if (products.isEmpty()) {
            System.out.println("No data to export.");
            return;
        }

        // Đặt tên file theo đinh dạng data_time_.csv
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = LocalDateTime.now().format(formatter);
        fileName = exportLocation + "\\"+"data_" + timestamp + ".csv";
        fileName_2 = "data_" + timestamp + ".csv";
        List<String> headersList = Arrays.asList(
                "Tên sản phẩm", "Giá", "Thương hiệu", "Loại", "Tình trạng",
                "CPU", "RAM", "Đĩa cứng", "Màn hình", "Card đồ họa",
                "Hệ điều hành", "Bảo hành", "Ngày nhập", "Ngày hết hạn"
        );

        count = 0; // Đếm số dòng đã ghi
        //  (7) Thêm dữ liệu vào File
        //  (8) Xuất dữ liệu ra file csv tới địa chỉ trong config
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            // Ghi tiêu đề vào file CSV
            fileWriter.write(String.join(",", headersList) + "\n");

            // Ghi dữ liệu từng dòng vào CSV
            for (Map<String, String> product : products) {
                List<String> row = new ArrayList<>();
                for (String header : headersList) {
                    String value = product.getOrDefault(header, "").replace("\"", "\"\"");
                    if (value.contains(",") || value.contains("\"")) {
                        value = "\"" + value + "\"";
                    }
                    row.add(value);
                }
                fileWriter.write(String.join(",", row) + "\n");
                count++;
            }

            System.out.println("Data exported to " + fileName + " successfully.");

            // Lấy kích thước file sau khi ghi
            File csvFile = new File(fileName);
            fileSizeKB = csvFile.length() / 1024.0;



        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
//            sendErrorEmail("Error writing to CSV file: " + e.getMessage());
        }
    }

    // Lấy địa chỉ email từ config
    private static String getEmailFromDatabase() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
         email = null;
        try {
            conn = JDBCUtil.getConnection();

            // Truy vấn lấy email từ bảng control ( config )
            String sql = "SELECT email FROM control WHERE id = 19";
            pstmt = conn.prepareStatement(sql);

            rs = pstmt.executeQuery();
            if (rs.next()) {
                email = rs.getString("email");
            }
        } catch (SQLException e) {
            System.err.println("Error fetching email from database: " + e.getMessage());
        } finally {
            JDBCUtil.closeConnection(conn); // Đóng kết nối
        }
        return email;
    }

    // (9) Gửi email báo thành công
    private static void sendSuccessEmail() {
        email = getEmailFromDatabase(); // Lấy email từ config
        if (email == null) {
            System.err.println("Failed to retrieve email from database.");
            return;
        }

        System.out.println("Send success notification email success");

        String subject = "Success Notification: Crawl data "; // Tiêu đề
        String message = "Scraping completed successfully!\n\n" + // Nội dung
                "Total products scraped: " + count + "\n" +
                "Export file location: " + fileName + "\n" +
                "File name: " + fileName_2 + "\n" +
                "Completion time: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        boolean sent = emailService.send(email, subject, message);
        if (!sent) {
            System.err.println("Failed to send success notification email");
        }
    }

    // (6) Gửi email báo lỗi
    private static void sendErrorEmail(String errorMessage) {
        email = getEmailFromDatabase(); // Lấy email từ config
        if (email == null) {
            System.err.println("Failed to retrieve email from database.");
            return;
        }

        System.out.println("Send error notification email success");
        String subject = "Error Notification: Crawl Laptopusa"; // Tiêu đề
        String message = "An error occurred during scraping:\n\n" + errorMessage; // Nội dung
        boolean sent = emailService.send(email, subject, message);
        if (!sent) {
            System.err.println("Failed to send error notification email");
        }
    }



    // (10) Ghi log nếu thành công export ra file csv
    private static void logSuccessToDatabase(String fileName, int count, double fileSizeKB) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = JDBCUtil.getConnection();

            // Chuẩn bị câu lệnh SQL cho việc chèn bản ghi thành công vào bảng log
            String sql = "INSERT INTO log (id_config, filename, date, event, status, count, file_size, dt_update, error_message) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1, 19);
            pstmt.setString(2, fileName);
            pstmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));
            pstmt.setString(4, "crawler data");
            pstmt.setString(5, "SU");
            pstmt.setInt(6, count);
            pstmt.setDouble(7, fileSizeKB);
            pstmt.setTimestamp(8, java.sql.Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(9, "");
            pstmt.executeUpdate();
            System.out.println("Logged success to database successfully.");
        } catch (SQLException e) {
            System.err.println("Error logging to database: " + e.getMessage());
        } finally {
            JDBCUtil.closeConnection(conn); // Đóng kết nối
        }
    }

    // (10) Ghi log nếu có lỗi
    private static void logErrorToDatabase(String errorMessage) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = JDBCUtil.getConnection();

            // Insert ghi lỗi vào bảng log
            String sql = "INSERT INTO log (id_config, filename, date, event, status, count, file_size, dt_update, error_message) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1, 19); // ID_config giả định là 1
            pstmt.setString(2, "");
            pstmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));
            pstmt.setString(4, "crawler data");
            pstmt.setString(5, "ER");
            pstmt.setNull(6, count);
            pstmt.setNull(7, (int) fileSizeKB);
            pstmt.setTimestamp(8, java.sql.Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(9, errorMessage);

            pstmt.executeUpdate();
            System.out.println("Logged error to database successfully.");
        } catch (SQLException e) {
            System.err.println("Error logging to database: " + e.getMessage());
        } finally {
            JDBCUtil.closeConnection(conn); // Đóng kết nối
        }
    }
}

