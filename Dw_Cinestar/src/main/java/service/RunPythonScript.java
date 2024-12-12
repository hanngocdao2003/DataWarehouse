package service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPythonScript {

    public String runScript(String scriptPath) {
        try {
            // Kiểm tra nếu Python có trong PATH hệ thống
            String pythonExecutable = "D:\\dowload\\Android\\python.exe";

            // Tạo ProcessBuilder với python và script path
            ProcessBuilder processBuilder = new ProcessBuilder(pythonExecutable, scriptPath);
            Process process = processBuilder.start();


            // Đọc output của lệnh Python (nếu có)
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            StringBuilder output = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            // Đọc error của lệnh Python (nếu có)
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((line = errorReader.readLine()) != null) {
                System.err.println(line);
            }

            // Chờ cho tiến trình Python hoàn tất và lấy mã thoát
            int exitCode = process.waitFor();

            // Nếu thành công, trả về output chứa tên file CSV
            if (exitCode == 0) {
                return output.toString().trim();  // Lấy tên file CSV từ output
            } else {
                return null;
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return null;  // Trả về null trong trường hợp có lỗi
        }
    }

//    public static void main(String[] args) {
//        // Đường dẫn tới script Python
//        String pythonScriptPath = "src/main/java/crawl/crawl1.py";
//
//        // Thực thi script và nhận kết quả (tên file CSV)
//        String csvFile = runScript(pythonScriptPath);
//
//        // In thông báo dựa trên kết quả chạy script
//        if (csvFile != null) {
//            System.out.println("Script executed successfully.");
//            System.out.println("CSV file generated: " + csvFile);  // In tên file CSV
//        } else {
//            System.out.println("Script failed.");
//        }
//    }
}
