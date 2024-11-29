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

            if(File_configsDAO.getInstance().checkProcess("N","F")) {
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
                                   // 1.6 kem tra them thanh cong or not
                                   fileConfigs = File_configsDAO.getInstance().check(id_config);
                               } catch (IOException e) {
                                   throw new RuntimeException(e);
                               }
                                System.out.println(fileConfigs);

                               // 1.7 Lấy id của file_config
                                int id_fileConfigs = (int) fileConfigs.getId();

                                System.out.println("ID file config: " + id_fileConfigs);

                                // 1.8 Run script crawl data
                                try {
                                    String csvFileName = runScript(fileConfigs.getSource_path());
                                    System.out.println(csvFileName);
                                } catch (IOException e) {
                                    throw new RuntimeException("Lỗi khi chạy script cho URL: " + fileConfigs.getSource_path(), e);
                                }
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

    public static String runScript(String urlSource) throws IOException {
        ETLService etlService = new ETLService();
        String csvFile = null;

        // Kiểm tra nếu URL khớp với "cinestart.com"
        if ("cinestar.com".equalsIgnoreCase(urlSource)) {
            System.out.println("Đang chạy script cho: " + urlSource);
            // Đường dẫn đến script Python
            String scriptPath = "src/main/java/crawl/crawl1.py";


            // Thực thi script và nhận kết quả (tên file CSV)
            csvFile = new RunPythonScript().runScript(scriptPath);


            if (csvFile != null) {

                System.out.println("Chạy script thành công, file CSV được tạo: " + csvFile);
                etlService.logFile("Chạy script thành công, file CSV được tạo: " + csvFile);
            } else {
                // Nếu chạy script không thành công
                System.out.println("Chạy script không thành công cho URL: " + urlSource);
                etlService.logFile("Chạy script không thành công cho URL: " + urlSource);
            }
        } else {
            // Nếu URL không hỗ trợ
            System.out.println("URL không được hỗ trợ: " + urlSource);
            etlService.logFile("URL không được hỗ trợ: " + urlSource);
        }

        return csvFile; // Trả về tên file nếu thành công, null nếu thất bại
    }
}
