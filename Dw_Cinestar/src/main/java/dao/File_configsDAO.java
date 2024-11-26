package dao;

import model.File_configs;
import service.ETLService;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class File_configsDAO {
    ETLService etlService = new ETLService();

    public static File_configsDAO getInstance() {
        return new File_configsDAO();
    }

    public boolean checkProcess(String status, String destination) throws IOException {
        String query = "SELECT status, destination " +
                "FROM file_datas " +
                "JOIN file_configs " +
                "ON file_configs.id = file_datas.id_config " +
                "WHERE status = ? AND destination = ?";

        try (PreparedStatement preparedStatement = etlService.getConnection("control").prepareStatement(query)) {
            // Gán giá trị vào các tham số của truy vấn
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, destination);

            // Thực thi truy vấn và xử lý kết quả
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    // Có bản ghi phù hợp
                    return true;
                } else {
                    // Không có bản ghi phù hợp
                    return false;
                }
            }
        } catch (SQLException e) {
            // Ghi log lỗi để kiểm tra
            System.out.println("Erro checkProcess" + e.getMessage());
            etlService.logFile("Erro checkProcess" + e.getMessage());
            return false;
        }
    }

    //test
    public static void main(String[] args) {
        try {
            // Tạo đối tượng ETLService để lấy kết nối
            ETLService etlService = new ETLService();

            // Tạo đối tượng DAO, truyền kết nối từ ETLService
            File_configsDAO fileConfigsDAO = new File_configsDAO();

            // Dữ liệu đầu vào cần kiểm tra
            String status = "N"; // Giá trị của trạng thái
            String destination = "F"; // Giá trị của đích đến

            // Gọi phương thức checkProcess và in kết quả
            boolean result = fileConfigsDAO.checkProcess(status, destination);

            if (result) {
                System.out.println("Found matching record with status: " + status + " and destination: " + destination);
                etlService.logFile("Found matching record with status: " + status + " and destination: " + destination);
            } else {
                System.out.println("No matching record found for status: " + status + " and destination: " + destination);
                etlService.logFile("No matching record found for status: " + status + " and destination: " + destination);
            }
        } catch (Exception e) {
            e.printStackTrace(); // Ghi log lỗi nếu có
        }
    }
}
