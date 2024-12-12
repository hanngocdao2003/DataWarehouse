package dao;

import log.MailTo;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class GetConnection {

    String driver = null;
    String url = null;
    String user = null;
    String pass = null;
    String databasebName = null;
    boolean checkE = false;

    public boolean getCheckE() {
        return checkE;
    }

    public void setCheckE(boolean check) {
        checkE = check;
    }

    public Connection getConnection(String location) throws IOException {
        String link = "D:\\DataWarehouse\\Dw_Cinestar\\src\\main\\resources\\config.properties";
        Connection result = null;

        if (location.equalsIgnoreCase("db_control")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);

                String driver = prop.getProperty("driver_local");
                String url = prop.getProperty("url_local");
                String databaseName = prop.getProperty("dbName_control");
                String user = prop.getProperty("user_local");
                String pass = prop.getProperty("pass_local");

                Class.forName(driver); // Nạp driver
                result = DriverManager.getConnection(url + "/" + databaseName, user, pass); // Kết nối DB

            } catch (Exception e) {
                System.out.println("Error reading file: " + link);
                logFile("Error reading file: " + link + "\n" + e.getMessage());
                MailTo.sendVerificationEmail(logFileToMail("Error reading file: " + link + "\n" + e.getMessage()));
                throw new IOException("Failed to establish connection", e); // Quăng ngoại lệ thay vì dừng chương trình
            }
        }else if (location.equalsIgnoreCase("db_staging")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);

                String driver = prop.getProperty("driver_local");
                String url = prop.getProperty("url_local");
                String databaseName = prop.getProperty("dbName_staging");
                String user = prop.getProperty("user_local");
                String pass = prop.getProperty("pass_local");

                Class.forName(driver); // Nạp driver
                result = DriverManager.getConnection(url + "/" + databaseName, user, pass); // Kết nối DB

            } catch (Exception e) {
                System.out.println("Error reading file: " + link);
                logFile("Error reading file: " + link + "\n" + e.getMessage());
                MailTo.sendVerificationEmail(logFileToMail("Error reading file: " + link + "\n" + e.getMessage()));
                throw new IOException("Failed to establish connection", e); // Quăng ngoại lệ thay vì dừng chương trình
            }
        }else if (location.equalsIgnoreCase("db_wh")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);

                String driver = prop.getProperty("driver_local");
                String url = prop.getProperty("url_local");
                String databaseName = prop.getProperty("dbName_datawarehouse");
                String user = prop.getProperty("user_local");
                String pass = prop.getProperty("pass_local");

                Class.forName(driver); // Nạp driver
                result = DriverManager.getConnection(url + "/" + databaseName, user, pass); // Kết nối DB

            } catch (Exception e) {
                System.out.println("Error reading file: " + link);
                logFile("Error reading file: " + link + "\n" + e.getMessage());
                MailTo.sendVerificationEmail(logFileToMail("Error reading file: " + link + "\n" + e.getMessage()));
                throw new IOException("Failed to establish connection", e); // Quăng ngoại lệ thay vì dừng chương trình
            }
        }else if (location.equalsIgnoreCase("db_mart")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);

                String driver = prop.getProperty("driver_local");
                String url = prop.getProperty("url_local");
                String databaseName = prop.getProperty("dbName_datamart");
                String user = prop.getProperty("user_local");
                String pass = prop.getProperty("pass_local");

                Class.forName(driver); // Nạp driver
                result = DriverManager.getConnection(url + "/" + databaseName, user, pass); // Kết nối DB

            } catch (Exception e) {
                System.out.println("Error reading file: " + link);
                logFile("Error reading file: " + link + "\n" + e.getMessage());
                MailTo.sendVerificationEmail(logFileToMail("Error reading file: " + link + "\n" + e.getMessage()));
                throw new IOException("Failed to establish connection", e); // Quăng ngoại lệ thay vì dừng chương trình
            }
        }

        return result;
    }

    public void logFile(String message) throws IOException {
        FileWriter fw = new FileWriter("D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\java\\log\\log.txt", true);
        PrintWriter pw = new PrintWriter(fw);
        pw.println(message + "\t");
        pw.println("HH:mm:ss dd/MM/yyyy - "
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")));
        pw.println("-----");
        pw.close();
    }

    public String logFileToMail(String message) throws IOException {
        StringBuilder re = new StringBuilder();
        re.append(message + "\t\n");
        re.append("HH:mm:ss dd/MM/yyyy - "
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")));
        re.append("-----");
        return re.toString();
    }


    public static void main(String[] args) throws IOException {
        GetConnection conn = new GetConnection();
        conn.getConnection("db_control");
        if(conn != null){
            System.out.println("Success");
        }
    }
}
