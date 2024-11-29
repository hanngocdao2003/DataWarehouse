package dao;

import model.File_configs;
import service.ETLService;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;

public class File_configsDAO {
    ETLService etlService = new ETLService();

    public static File_configsDAO getInstance() {
        return new File_configsDAO();
    }

    public boolean checkProcess(String status, String destination) throws IOException {
        String query = "SELECT status, destination " +
                "FROM control.file_datas " +
                "JOIN control.file_configs " +
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

    public File_configs check(int id_config) throws IOException {
        File_configs fileConfigs = new File_configs();
        String query = "SELECT * " +
                "FROM control.file_configs " +
                "WHERE id = ? ";

        try (PreparedStatement preparedStatement = etlService.getConnection("control").prepareStatement(query)) {
            // Gán giá trị vào các tham số của truy vấn
            preparedStatement.setInt(1, id_config);

            // Thực thi truy vấn và xử lý kết quả
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    // Có bản ghi phù hợp
                    int id = resultSet.getInt("id");
                    String descript = resultSet.getString("descript");
                    String source_path = resultSet.getString("source_path");
                    String source_location = resultSet.getString("source_location");
                    String format = resultSet.getString("format");
                    String seperator = resultSet.getString("seperator");
                    String colums = resultSet.getString("colums");
                    String destination = resultSet.getString("destination");
                    Timestamp created_at = resultSet.getTimestamp("created_at");
                    Timestamp update_at = resultSet.getTimestamp("update_at");
                    String create_by = resultSet.getString("create_by");
                    String update_by = resultSet.getString("update_by");

                    fileConfigs = new File_configs(id,descript,source_path,source_location,
                            format,seperator,colums,destination,created_at,update_at,create_by,update_by);
                }
            }
        } catch (SQLException e) {
            // Ghi log lỗi để kiểm tra
            System.out.println("Erro check" + e.getMessage());
            etlService.logFile("Erro check" + e.getMessage());
        }
        return fileConfigs;
    }


    // 1.5. thêm source vào bảng file_configs
    public int addFile_configs(String source_url,String folder) throws IOException  {
        long id_config = -1;
        String query = "INSERT INTO control.file_configs (description, source_path, location, format, `seperator`, colums, destination, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = etlService.getConnection("control").prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            String source_path = source_url;
            String location = folder;
            Timestamp createdAt = Timestamp.from(Instant.now()); // Lấy ngày hiện tại
            Timestamp updatedAt = null; // Chưa có thông tin về ngày cập nhật
            String createdBy = "Duong";
            String updatedBy = null; // Chưa có thông tin về người cập nhật

            // Thay đổi dữ liệu này dựa trên cấu trúc bảng và cột
            preparedStatement.setString(1, "nguồn lấy dữ liệu"); // description
            preparedStatement.setString(2,source_path ); // source_path
            preparedStatement.setString(3,location ); // location
            preparedStatement.setString(4, ".csv"); // format
            preparedStatement.setString(5, ","); // separator
            preparedStatement.setString(6, "24"); // columns
            preparedStatement.setString(7, "F"); // destination
            preparedStatement.setTimestamp(8, createdAt);
            preparedStatement.setTimestamp(9,  null);
            preparedStatement.setString(10, createdBy); // created_by
            preparedStatement.setString(11, updatedBy); // updated_by

            // Thực hiện chèn dữ liệu
            int affectedRows = preparedStatement.executeUpdate();

            if (affectedRows > 0) {
                // Lấy ID được tạo tự động
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        id_config = generatedKeys.getInt(1);
                    }
                }
            }


        } catch (SQLException | IOException e) {
            // Ghi log lỗi để kiểm tra
            System.out.println("Erro addFile_configs" + e.getMessage());
            etlService.logFile("Erro addFile_configs" + e.getMessage());
        }
        return (int) id_config;
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
