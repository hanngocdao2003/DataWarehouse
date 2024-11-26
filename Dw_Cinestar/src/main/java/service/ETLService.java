package service;

import config.ConfigLoader;
import dao.DatabaseConnection;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class ETLService {

    private static final String Log_PATH = "src/main/java/log/log.txt";
    boolean checkE = false;

    public boolean isCheckE() {
        return checkE;
    }

    public void setCheckE(boolean checkE) {
        this.checkE = checkE;
    }

    // log file
    public void logFile(String message) throws IOException {

        FileWriter writeLog = new FileWriter(Log_PATH, true);
        PrintWriter printLog = new PrintWriter(writeLog);
        printLog.println(message + "\t");
        printLog.println("HH:mm:ss dd/MM/yyyy - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")));
        printLog.close();
    }

    // 2. ket noi db
    public Connection getConnection(String location) throws IOException {
        Connection result = null;

        ConfigLoader configLoader = new ConfigLoader();
        Properties prop = configLoader.loadConfig();

        // lay thuoc tinh cau hinh trong file config
        String driver = prop.getProperty("driver_local");
        String url = prop.getProperty("url_local");
        String dbName = "";
        String user = prop.getProperty("user_local");
        String pass = prop.getProperty("pass_local");

        //
        if (location.equalsIgnoreCase("control")) {
            dbName = prop.getProperty("dbName_control");
        } else if (location.equalsIgnoreCase("staging")) {
            dbName = prop.getProperty("dbName_staging");
        }

        DatabaseConnection dbConnection = new DatabaseConnection();
        try {
            result = dbConnection.connect(driver, url, dbName, user, pass);
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Error connecting to database: " + location + e.getMessage());
            updateErro("Erro connect: " + e.getLocalizedMessage(), location);
            System.exit(0);
        }
        return result;
    }

    //
    public boolean updateErro(String message, String location) throws IOException{
        if(location.equalsIgnoreCase("control")){
            // log file
            logFile(message);
            checkE = false;
            System.exit(0);
        }
        else {
            // chuyen trang thai
            checkE = true;
        }
        return checkE;
    }

    // test
    public static void main(String[] args) {
        ETLService etlService = new ETLService();

        try {
            // Kiểm tra kết nối với database "control"
            System.out.println("Đang kết nối đến database 'control'...");
            etlService.logFile("This is a test log message.");
            Connection controlConnection = etlService.getConnection("control");
            if (controlConnection != null) {
                System.out.println("Kết nối thành công tới database 'control'.");
                controlConnection.close(); // Đóng kết nối sau khi kiểm tra
            }


        } catch (IOException e) {
            System.err.println("Lỗi khi tải file cấu hình hoặc ghi log: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Lỗi khác: " + e.getMessage());
        }
    }
}



