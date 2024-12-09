package dao;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

public class GetConnection2 {
    String driver = null;
    String url = null;
    String user = null;
    String pass = null;
    String databaseName = null;
    public static int checkE;

    public Connection getConnection(String location) throws IOException {
        // path config src\test\config.properties
        // path config ./module/config/config.properties
//		 String link = "/test/config.properties";
        String filePath = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\resources\\config.properties";

        // Sử dụng ClassLoader để đọc tệp tin
        ClassLoader classLoader = GetConnection2.class.getClassLoader();

//		String link = ".\\src\\test\\config.properties"; classLoader.getResourceAsStream(filePath)
        Connection result = null;
        // ket noi db warehouse
        if (location.equalsIgnoreCase("db_wh")) {

            try (InputStream input = new FileInputStream(filePath)) {

//			try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);
                // lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databaseName = prop.getProperty("dbName_datawarehouse");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");


            } catch (IOException e) {
                Timestamp date = new Timestamp(System.currentTimeMillis());
                String date_err = date.toString();
                String fileName = "D:\\logs\\logERR-" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";

                PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
                writer.println("Error: " + e.getMessage());
                writer.close();
            }
            // ket noi db mart local
        } else if (location.equalsIgnoreCase("db_mart")) {
            try (InputStream input = new FileInputStream(filePath)) {
//				try (InputStream input = new FileInputStream(link)) {

                Properties prop = new Properties();
                prop.load(input);
                // 2.2.1 lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databaseName = prop.getProperty("dbName_datamart");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");


            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else if (location.equalsIgnoreCase("db_control")) {
            try (InputStream input = new FileInputStream(filePath)) {
//				try (InputStream input = new FileInputStream(link)) {

                Properties prop = new Properties();
                prop.load(input);
                // lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databaseName = prop.getProperty("dbName_control");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");


            } catch (IOException ex) {
                ex.printStackTrace();
            }
            // ket noi db mart server
//		}else if (location.equalsIgnoreCase("mart")) {
//			try (InputStream input = new FileInputStream(link)) {
//				Properties prop = new Properties();
//				prop.load(input);
//				// 2.2.1 lấy từng thuộc tính cấu hình trong file config
//				driver = prop.getProperty("driver_local");
//				url = prop.getProperty("url_server");
//				databaseName = prop.getProperty("dbName_datamart");
//				user = prop.getProperty("username_server");
//				pass = prop.getProperty("pass_server");
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}
        }

        try {
            // đăng kí driver
            Class.forName(driver); // Nạp driver

            try {
//				2. Kết nối db control
                result = DriverManager.getConnection(url + "/" + databaseName, user, pass); // Kết nối DB
                checkE = 1;
            } catch (SQLException e) {
//              2.1 Tạo file ghi lỗi

                if (location.equalsIgnoreCase("control")) {
                    System.out.println("Kết nối không thành công");
                    Timestamp date = new Timestamp(System.currentTimeMillis());
                    String date_err = date.toString();
                    String fileName = "D:\\logs\\logERR-" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";

                    PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
                    writer.println("Error: " + e.getMessage());
                    e.printStackTrace(writer);
                    writer.close();
                    System.exit(0);
                }
                checkE = 0;
            }

        } catch (NullPointerException | ClassNotFoundException e) {
            Timestamp date = new Timestamp(System.currentTimeMillis());
            String date_err = date.toString();
            String fileName = "D:\\logs\\logERR-" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";

            PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
            writer.println("Error: " + e.getMessage());
            e.printStackTrace(writer);
            writer.close();
            System.out.println("Không kết nối được driver");
            e.printStackTrace();
            System.exit(0);
        }
        return result;
    }
    public static void main(String[] args) {
        GetConnection2 connectionManager = new GetConnection2();

        // Test connection for different database locations
        String[] locations = {"db_wh", "db_mart", "db_control"};

        for (String location : locations) {
            System.out.println("Testing connection for location: " + location);
            try (Connection connection = connectionManager.getConnection(location)) {
                if (connection != null && !connection.isClosed()) {
                    System.out.println("Connection to " + location + " successful.");
                    System.out.println(checkE);
                } else {
                    System.out.println("Connection to " + location + " failed.");
                }
            } catch (IOException | SQLException e) {
                System.out.println("Error while connecting to " + location + ": " + e.getMessage());
            }
            System.out.println("-----------------------------------------------------");
        }
    }
}
