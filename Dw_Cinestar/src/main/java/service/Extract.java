package service;

import config.ConfigLoader;
import dao.File_configsDAO;
import model.File_configs;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extract {

    public static void main(String[] args) throws IOException {
        ETLService etlService = new ETLService();

        //1.1 doc file config.properties
        ConfigLoader configLoader = new ConfigLoader();

        try {
            Properties properties = configLoader.loadConfig(); // Gọi phương thức loadConfig
            System.out.println("Config properties loaded successfully.");

            // 1.2. Ket noi db
            etlService.getConnection("control");

            // 1.3. Ktra tien trinh dang chay
            Extract extract = new Extract();

            File_configsDAO file_configsDAO = new File_configsDAO();
            if(file_configsDAO.checkProcess("P","F")) {
                System.out.println("Process is running");
                etlService.logFile("Process is running");

                //1.4. Load source config & 1.5. thêm soure vào table file_configs
                List<Integer> ids_config = configLoader.processConfigFile();
                System.out.println(ids_config);
                ExecutorService executor = Executors.newFixedThreadPool(2);
                for(Integer id_config : ids_config) {
                    executor.submit(
                            () -> {
                                File_configs fileConfigs = null;
                               try {
                                   // 1.5 thanh cong
                                   fileConfigs = File_configsDAO.getInstance().check(id_config);
                               } catch (IOException e) {
                                   throw new RuntimeException(e);
                               }
                                System.out.println(fileConfigs);
                            }
                    );
                }

            }
            else {
                System.out.println("No process is running");
                etlService.logFile("No process is running");
            }

        } catch (Exception e) {
            System.err.println("Error loading configuration");
            etlService.logFile("Error loading configuration: " + e.getMessage());
        }

    }

}
