package dao;

import config.ConfigLoader;
import model.File_configs;
import service.ETLService;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.Properties;

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
        String query = "INSERT INTO control.file_configs (descript, source_path, source_location, format, seperator, colums, destination, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = etlService.getConnection("control").prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            String source_path = source_url;
            String location = folder;
            Timestamp created_at = Timestamp.from(Instant.now()); // Lấy ngày hiện tại
            Timestamp update_at = null; // Chưa có thông tin về ngày cập nhật
            String create_by = "Duong";
            String update_by = null; // Chưa có thông tin về người cập nhật

            // Thay đổi dữ liệu này dựa trên cấu trúc bảng và cột
            preparedStatement.setString(1, "nguồn lấy dữ liệu"); // description
            preparedStatement.setString(2,source_path ); // source_path
            preparedStatement.setString(3,location ); // location
            preparedStatement.setString(4, ".csv"); // format
            preparedStatement.setString(5, ","); // separator
            preparedStatement.setString(6, "24"); // columns
            preparedStatement.setString(7, "F"); // destination
            preparedStatement.setTimestamp(8, created_at);
            preparedStatement.setTimestamp(9,  update_at);
            preparedStatement.setString(10, create_by);
            preparedStatement.setString(11, update_by);

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

    //test add_config
    public static void main(String[] args) {
        try {
            // Tạo đối tượng ETLService để lấy kết nối
            ETLService etlService = new ETLService();

            // Tạo đối tượng DAO, truyền kết nối từ ETLService
            File_configsDAO fileConfigsDAO = new File_configsDAO();

            // Dữ liệu thử nghiệm cần thêm vào bảng file_configs
            ConfigLoader configLoader = new ConfigLoader();
            Properties prop = configLoader.loadConfig();
            String sourceUrl = prop.getProperty("url_source");
            String folder = prop.getProperty("folder_location");

            // Gọi phương thức addFile_configs để thêm dữ liệu vào bảng
            int idConfig = fileConfigsDAO.addFile_configs(sourceUrl, folder);

            // Kiểm tra kết quả trả về từ phương thức addFile_configs
            if (idConfig != -1) {
                System.out.println("Dữ liệu đã được thêm thành công vào bảng file_configs với ID: " + idConfig);
                etlService.logFile("Dữ liệu đã được thêm thành công vào bảng file_configs với ID: " + idConfig);
            } else {
                System.out.println("Thêm dữ liệu không thành công.");
                etlService.logFile("Thêm dữ liệu không thành công.");
            }

        } catch (Exception e) {
            e.printStackTrace(); // Ghi log lỗi nếu có
        }
    }

}
