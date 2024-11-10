package crawl;

import Email.EmailProperty;
import Email.EmailService;
import Email.IJavaMail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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

public class CrawlProduct {

    // 1. Khởi tạo các biến trong bảng config
    private static String sourceUrl;
    private static String exportLocation;
    private static final String BASE_URL = "https://laptopusa.vn";
    // Biến theo dõi trạng thái email
    private static boolean successEmailSent = false;
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
        System.out.println("Loading configuration...");

        // 1.1 Tải cấu hình từ file YAML
        if (!loadConfig("D:/warehouse/control/config.yaml.txt")) {
            System.err.println("Failed to load configuration.");
            return;
        }

        System.out.println("Starting the scraping process...\n");
        List<Map<String, String>> allProducts = new ArrayList<>();
        int page = 1;

        // 2. Duyệt qua các liên kết trong trang laptopusa.vn
        while (true) {
            String collectionUrl = sourceUrl + "?page=" + page;
            System.out.println("Fetching product links from " + collectionUrl + "...");

            // 3. Lấy ra danh sách liên kết của các sản phẩm ( page )
            List<String> productLinks = getProductLinks(collectionUrl);
            // Nếu không có thì break
            if (productLinks.isEmpty()) {
                System.out.println("No more products found on page " + page + ". Stopping.");
//                sendErrorEmail("No products found on page " + page);
                break;
            }

            // 4. Duyệt vòng lặp qua danh sách liên kết, lấy dữ liệu các sản phẩm
            for (String link : productLinks) {
                Map<String, String> productDetails = scrapeProductDetails(link);
                if (productDetails != null) {
                    allProducts.add(productDetails);
                }
            }
            // 5. Nếu không có dữ liệu sản phẩm thì gửi thông báo lỗi qua email và break
            // Nếu có dữ liệu sản phẩm thì tiếp tục
            if (allProducts.isEmpty()) {
                System.out.println("No product data found from the links. Sending error email...");
                sendErrorEmail("No product data found on page " + page);

                logErrorToYAML("Không tìm thấy dữ liệu của sản phẩm");

                break;
            }
            page++;

        }

        // Xuất File ( bao gồm bước 6 và 7 )
        exportToCSV(allProducts);
        // 8. Ghi log sau khi export CSV thành công
        logSuccessToYAML(fileName_2, count, fileSizeKB);
        System.out.println("\nScraping completed.");
    }

    // Tải cấu hình từ file config
    // Lấy địa chỉ lưu file trong config và gán cho exportLocation
    private static boolean loadConfig(String filePath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> config = mapper.readValue(new File(filePath), Map.class);

            // Lấy thông tin cấu hình từ file config
            sourceUrl = (String) ((Map) config.get("ID_1")).get("Source");
            exportLocation = (String) ((Map) config.get("ID_19")).get("Source_File_Location");
            new File(exportLocation).mkdirs();

            return true;
        } catch (IOException e) {
            System.err.println("Error reading YAML config: " + e.getMessage());
//            sendErrorEmail("Error reading YAML config: " + e.getMessage());
        }
        return false;
    }

    // (3) Phương thức lấy ra danh sách liên kết của các sản phẩm
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

    // (4) Phương thức Lấy dữ liệu chi tiết của sản phẩm
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

    // Xuất dữ liệu ra file CSV
    // Gốm (6) lấy địa chỉ file CSV trong config và
    // (7) Thêm dữ liệu vào file
    private static void exportToCSV(List<Map<String, String>> products) {
        if (products.isEmpty()) {
            System.out.println("No data to export.");
            return;
        }

        // 6. Lấy địa chỉ file CSV trong config
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = LocalDateTime.now().format(formatter);
        fileName = exportLocation + "\\"+"data_" + timestamp + ".csv";
        fileName_2 = "data_" + timestamp + ".csv";
        List<String> headersList = Arrays.asList(
                "Tên sản phẩm", "Giá", "Thương hiệu", "Loại", "Tình trạng",
                "CPU", "RAM", "Đĩa cứng", "Màn hình", "Card đồ họa",
                "Hệ điều hành", "Bảo hành", "Ngày nhập", "Ngày hết hạn"
        );

        count = 0;
        // 7. Thêm dữ liệu vào File
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
            sendErrorEmail("Error writing to CSV file: " + e.getMessage());
        }
    }


    private static void sendErrorEmail(String errorMessage) {
        String subject = "Error Notification: Crawl Laptopusa";
        String message = "An error occurred during scraping:\n\n" + errorMessage;
        boolean sent = emailService.send(EmailProperty.APP_EMAIL, subject, message);
        if (!sent) {
            System.err.println("Failed to send error notification email");
        }
    }

    // Ghi log nếu thành công export ra file csv
    private static void logSuccessToYAML(String fileName, int count, double fileSizeKB) {
        String filePath = "D:/warehouse/control/log.yaml.txt";
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> yamlData;

            // Đọc nội dung hiện có từ file YAML (nếu có)
            File file = new File(filePath);
            if (file.exists()) {
                yamlData = mapper.readValue(file, Map.class);
            } else {
                yamlData = new HashMap<>();
            }

            // Lấy danh sách các bản ghi nếu có
            List<Map<String, Object>> logs = (List<Map<String, Object>>) yamlData.getOrDefault("file_log", new ArrayList<>());

            // Lấy ID lớn nhất hiện tại và tăng ID cho bản ghi mới
            int maxId = logs.stream()
                    .map(log -> (int) log.get("ID"))
                    .max(Integer::compare)
                    .orElse(0);

            // Tạo bản ghi mới
            Map<String, Object> newLog = new LinkedHashMap<>();
            newLog.put("ID", maxId + 1);
            newLog.put("ID_config", 1);
            newLog.put("filename", fileName);
            newLog.put("date", LocalDate.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy")));
            newLog.put("event", "Data export successful");
            newLog.put("status", "SC");
            newLog.put("count", count);
            newLog.put("file_size (kb)", String.format("%.2f", fileSizeKB)); // làm tròn đến 2 chữ số thập phân
            newLog.put("dt_update", LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm dd/MM/yyyy")));

            // Thêm bản ghi mới vào danh sách và ghi lại vào YAML
            logs.add(newLog);
            yamlData.put("file_log", logs);
            mapper.writeValue(file, yamlData);

            System.out.println("Logged success to YAML file successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to YAML file: " + e.getMessage());
//            sendErrorEmail("Error writing to YAML file: " + e.getMessage());
        }
    }


    // Ghi log nếu có lỗi
    private static void logErrorToYAML(String errorMessage) {
        String filePath = "D:/warehouse/control/log.yaml.txt";
        try {
            // Đọc nội dung hiện có từ file YAML
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> yamlData;

            File file = new File(filePath);
            if (file.exists()) {
                yamlData = mapper.readValue(file, Map.class);
            } else {
                yamlData = new HashMap<>();
            }

            // Tạo danh sách các bản ghi nếu chưa có
            List<Map<String, Object>> logs = (List<Map<String, Object>>) yamlData.getOrDefault("file_log", new ArrayList<>());

            // Tìm ID hiện tại lớn nhất và tăng ID cho bản ghi mới
            int maxId = logs.stream()
                    .map(log -> (int) log.get("ID"))
                    .max(Integer::compare)
                    .orElse(0);

            // Tạo bản ghi mới
            Map<String, Object> newLog = new LinkedHashMap<>();
            newLog.put("ID", maxId + 1);
            newLog.put("ID_config", 1);
            newLog.put("filename", "");
            newLog.put("date", LocalDate.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy")));
            newLog.put("event", "Product data not found");
            newLog.put("status", "ER");
            newLog.put("count", count);
            newLog.put("file_size (kb)", String.format("%.2f", fileSizeKB)); // làm tròn đến 2 chữ số thập phân
            newLog.put("dt_update", LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm dd/MM/yyyy")));
            newLog.put("error_message", errorMessage);

            // Thêm bản ghi mới vào danh sách và lưu lại vào file YAML
            logs.add(newLog);
            yamlData.put("file_log", logs);
            mapper.writeValue(file, yamlData);

            System.out.println("Logged error to YAML file successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to YAML file: " + e.getMessage());
        }
    }

}