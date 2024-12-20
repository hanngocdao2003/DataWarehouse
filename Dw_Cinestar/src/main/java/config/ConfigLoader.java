//package config;
//
//import dao.DatabaseConnection;
////import dao.File_configsDAO;
//import service.ETLService;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//
//public class ConfigLoader {
//    ETLService log = new ETLService();
//    private static final String CONFIG_PATH = "config.properties";
//
//    public Properties loadConfig() throws IOException {
//        Properties prop = new Properties();
//
//        // Load file từ classpath
//        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_PATH)) {
//            prop.load(input);
//        } catch (IOException e) {
//                System.out.println("Unknown file " + CONFIG_PATH);
//                log.logFile("Unknown file " + CONFIG_PATH + "\n" + e.getMessage());
//                System.exit(0);
//        }
//
//        return prop;
//    }
//
//    // Đọc và xử lý cấu hình từ tệp properties
//    public int processConfigFile() throws IOException {
//
//        InputStream input = null;
//        long id_config = -1;
//        try {
//            ConfigLoader configLoader = new ConfigLoader();
//            Properties prop = configLoader.loadConfig();
//
//            // Lấy giá trị của khóa url_source
//            String urlSourceValue = prop.getProperty("url_source");
//            String urlForderLocation = prop.getProperty("folder_location");
//
//            // 1.5. thêm source vào bảng file_configs
//             id_config = File_configsDAO.getInstance().addFile_configs(urlSourceValue, urlForderLocation);
//            System.out.println("Inserted data_file_configs row with ID: " + id_config);
//            System.out.println("urlForderLocation: " + urlForderLocation);
//        } catch (IOException e) {
//            //1.5 log khi khong thanh cong
//            log.logFile("Add config source Failed");
//        }
//
//        return (int) id_config;
//    }
//
////    public String getProperty(String key) {
////        return properties.getProperty(key);
////    }
//
//
//    //test
//public static void main(String[] args) throws SQLException {
//    // Tạo đối tượng ConfigLoader hoặc lớp có chứa phương thức processConfigFile
//    ConfigLoader configLoader = new ConfigLoader(); // Giả sử ConfigLoader chứa phương thức processConfigFile
//
//    try {
//        // Gọi phương thức processConfigFile để thêm cấu hình vào cơ sở dữ liệu
//       int id = configLoader.processConfigFile();
//
//        // In ra ID đã được thêm vào cơ sở dữ liệu
//        if (id != 0) {
//            System.out.println("Successfully added the following config ID: ");
//
//                System.out.println("Config ID: " + id);
//
//        } else {
//            System.out.println("No config ID were added.");
//        }
//
//    } catch (IOException e) {
//        System.err.println("IOException occurred while processing config file: " + e.getMessage());
//    }
//}
//}
//
