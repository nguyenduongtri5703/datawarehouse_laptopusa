package test;

import crawl.CrawlProduct;
import database.JDBCUtil;
import load_to_warehouse.LoadToWarehouse;

import java.sql.Connection;

public class Test {
    public static void main(String[] args) {
        //1. Kết nối DB
//        Connection connection = JDBCUtil.getConnection();

        //2. Crawl data
// CrawlProduct.runCrawl();

        //3. Load to staging
//  DataProcessing.loadToStaging();

        //4. Load to warehouse
       LoadToWarehouse.loadToWareHouse();

        //5. Đóng kết nối
//        JDBCUtil.closeConnection(connection);
    }
}
