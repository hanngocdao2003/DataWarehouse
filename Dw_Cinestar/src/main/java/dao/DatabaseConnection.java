package dao;

import service.ETLService;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    ETLService log;
    String driver = null;
    String url = null;
    String user = null;
    String pass = null;
    String dbName = null;

    public Connection connect(String driver, String url, String dbName, String user, String pass) throws ClassNotFoundException, SQLException, IOException {
        // Đăng ký driver
        String connectionURL = null;
        try {
            Class.forName(driver);
            connectionURL = url + dbName;
        } catch (ClassNotFoundException e) {
            System.out.println("Cannot connect Driver");
            log.logFile("Cannot connect Driver" + "\n" + e.getMessage());
            System.exit(0);
        }

        // Kết nối DB
        return DriverManager.getConnection(connectionURL, user, pass);
    }
    // Hàm main để test kết nối
//    public static void main(String[] args) {
//        DatabaseConnection dbConnection = new DatabaseConnection();
//        // Thông tin kết nối MySQL
//        String driver = "com.mysql.cj.jdbc.Driver"; // Driver JDBC của MySQL
//        String url = "jdbc:mysql://localhost:3306/"; // URL kết nối
//        String dbName = "control";
//        String user = "root";
//        String pass = "root";
//
//        try {
//            // Thử kết nối
//            Connection connection = dbConnection.connect(driver, url, dbName, user, pass);
//            if (connection != null) {
//                System.out.println("Kết nối thành công đến cơ sở dữ liệu: " + dbName);
//                connection.close(); // Đóng kết nối sau khi kiểm tra
//            }
//        } catch (ClassNotFoundException e) {
//            System.err.println("Lỗi driver JDBC: " + e.getMessage());
//        } catch (SQLException e) {
//            System.err.println("Lỗi SQL: " + e.getMessage());
//        } catch (IOException e) {
//            System.err.println("Lỗi IO: " + e.getMessage());
//        }
//    }
}
