package config;

import service.ETLService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
