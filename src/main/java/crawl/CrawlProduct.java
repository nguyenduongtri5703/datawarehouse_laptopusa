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
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
    private static int count;
    // Kích thước file
    private static double fileSizeKB;
    // Khởi tạo EmailService
    private static final IJavaMail emailService = new EmailService();

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

    // (2) Tải cấu hình từ file config
    // Lấy địa chỉ lưu file trong config và gán cho exportLocation
    private static boolean loadConfigFromDatabase() {
        Connection conn = null;
        CallableStatement cstmt = null;

        try {
            conn = JDBCUtil.getConnection();

            // Gọi stored procedure
            String storedProc = "{CALL load_config_data(?, ?, ?, ?)}";
            cstmt = conn.prepareCall(storedProc);

            // Thiết lập tham số đầu vào
            cstmt.setInt(1, 19); // ID của cấu hình cần lấy


            cstmt.registerOutParameter(2, java.sql.Types.VARCHAR); // source
            cstmt.registerOutParameter(3, java.sql.Types.VARCHAR); // source_location
            cstmt.registerOutParameter(4, java.sql.Types.VARCHAR); // email

            // Thực thi stored procedure
            cstmt.execute();

            // Lấy kết quả từ các tham số đầu ra
            String source = cstmt.getString(2);
            String sourceLocation = cstmt.getString(3);
            String emailConfig = cstmt.getString(4);

            if (source != null && sourceLocation != null && emailConfig != null) {
                // Gán giá trị cho các biến global
                sourceUrl = source;
                exportLocation = sourceLocation;
                email = emailConfig; // email

                // Tạo thư mục nếu chưa tồn tại
                new File(exportLocation).mkdirs();

                System.out.println("Configuration loaded successfully!");
                return true;
            } else {
                System.err.println("Configuration not found in database.");
            }
        } catch (SQLException e) {
            System.err.println("Error loading configuration from database: " + e.getMessage());
        } finally {
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
        fileName = exportLocation + "\\" + "data_" + timestamp + ".csv";
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
                    // Xử lý giá trị nếu là cột "Giá", đổi thành định dạng decimal
                    if ("Giá".equals(header)) {
                        if (value == null || value.isEmpty() || "Not Found".equals(value)) {
                            value = "0.00"; // Gán giá trị mặc định nếu không hợp lệ
                        } else {
                            try {
                                // Loại bỏ ký tự tiền tệ, dấu phẩy, khoảng trắng
                                String cleanedPrice = value.replaceAll("[^\\d.]", "").replace(",", "");
                                // Chuyển thành số thập phân và định dạng
                                double decimalPrice = Double.parseDouble(cleanedPrice);
                                value = String.format("%.2f", decimalPrice);
                            } catch (NumberFormatException e) {
                                System.err.println("Lỗi khi chuyển đổi giá: " + value);
                                value = "0.00"; // Giá trị mặc định nếu chuyển đổi thất bại
                            }
                        }
                    }

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
    // (9) Gửi email báo thành công
    private static void sendSuccessEmail() {
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
        CallableStatement cstmt = null;
        try {
            // Lấy kết nối từ JDBCUtil
            conn = JDBCUtil.getConnection();

            // Gọi stored procedure
            String storedProc = "{CALL log_crawl(?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            cstmt = conn.prepareCall(storedProc);

            // Thiết lập các tham số cho stored procedure
            cstmt.setInt(1, 19); // id_config
            cstmt.setString(2, fileName); // filename
            cstmt.setDate(3, java.sql.Date.valueOf(LocalDate.now())); // date
            cstmt.setString(4, "crawler data"); // event
            cstmt.setString(5, "SU"); // status
            cstmt.setInt(6, count); // count
            cstmt.setDouble(7, fileSizeKB); // file_size
            cstmt.setTimestamp(8, java.sql.Timestamp.valueOf(LocalDateTime.now())); // dt_update
            cstmt.setString(9, ""); // error_message

            // Thực thi stored procedure
            cstmt.execute();
            System.out.println("Logged success to database");
        } catch (SQLException e) {
            System.err.println("Error while logging to database: " + e.getMessage());
        } finally {
            // Đóng CallableStatement và Connection
            if (cstmt != null) {
                try {
                    cstmt.close();
                } catch (SQLException e) {
                    System.err.println("Error closing CallableStatement: " + e.getMessage());
                }
            }
            JDBCUtil.closeConnection(conn);
        }
    }


    // (10) Ghi log nếu có lỗi
    private static void logErrorToDatabase(String errorMessage) {
        Connection conn = null;
        CallableStatement cstmt = null;
        try {
            // Lấy kết nối từ JDBCUtil
            conn = JDBCUtil.getConnection();

            // Gọi stored procedure
            String storedProc = "{CALL log_crawl(?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            cstmt = conn.prepareCall(storedProc);

            // Thiết lập các tham số cho stored procedure
            cstmt.setInt(1, 19); // id_config
            cstmt.setString(2, ""); // tên file
            cstmt.setDate(3, java.sql.Date.valueOf(LocalDate.now())); // date
            cstmt.setString(4, "crawler data");
            cstmt.setString(5, "ER"); // trạng thái
            cstmt.setInt(6, count); // count
            cstmt.setInt(7, (int) fileSizeKB); // file_size// kích thước file
            cstmt.setTimestamp(8, java.sql.Timestamp.valueOf(LocalDateTime.now()));
            cstmt.setString(9, errorMessage);

            // Thực thi stored procedure
            cstmt.execute();
            System.out.println("Logged error to database successfully");
        } catch (SQLException e) {
            System.err.println("Error while logging to database: " + e.getMessage());
        } finally {
            // Đóng CallableStatement và Connection
            if (cstmt != null) {
                try {
                    cstmt.close();
                } catch (SQLException e) {
                    System.err.println("Error closing CallableStatement: " + e.getMessage());
                }
            }
            JDBCUtil.closeConnection(conn);
        }
    }
}

