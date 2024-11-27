package config;

import dao.DatabaseConnection;
import dao.File_configsDAO;
import service.ETLService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConfigLoader {
    ETLService log;
    private static final String CONFIG_PATH = "config.properties";

    public Properties loadConfig() throws IOException {
        Properties prop = new Properties();

        // Load file từ classpath
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_PATH)) {
            prop.load(input);
        } catch (IOException e) {
                System.out.println("Unknown file " + CONFIG_PATH);
                log.logFile("Unknown file " + CONFIG_PATH + "\n" + e.getMessage());
                System.exit(0);
        }

        return prop;
    }

    // Đọc và xử lý cấu hình từ tệp properties
    public List<Integer> processConfigFile() throws SQLException, IOException {

        List<Integer> ids_config = new ArrayList<>();

        InputStream input = null;
        try {
            Properties properties = new Properties();
            input = new FileInputStream(CONFIG_PATH);
            properties.load(input);

            // Lấy giá trị của khóa url_source
            String urlSourceValue = properties.getProperty("url_source");
            String urlForderLocation = properties.getProperty("folder_location");

            // Phân tách các giá trị theo dấu phẩy
            List<String> urlList = Arrays.asList(urlSourceValue.split(","));

            // In ra các giá trị
            for (String url : urlList) {
                System.out.println("URL: " + url);

                // 1.5. thêm source vào bảng file_configs
                int id_config = File_configsDAO.getInstance().addFile_configs(url, urlForderLocation);
                System.out.println("Inserted data_file_configs row with ID: " + id_config);
                ids_config.add(id_config);
            }
            System.out.println("urlForderLocation: " + urlForderLocation);
        } catch (IOException e) {

            log.logFile("Add config source Failed");
        }

        return ids_config;
    }

//    public String getProperty(String key) {
//        return properties.getProperty(key);
//    }


    //test
public static void main(String[] args) {
    ConfigLoader configLoader = new ConfigLoader();
    try {
        Properties prop = configLoader.loadConfig();
        // In ra một thuộc tính ví dụ
        System.out.println("Database URL: " + prop.getProperty("url_local"));
    } catch (IOException e) {
        System.err.println("Error loading configuration: " + e.getMessage());
    }
}
}
