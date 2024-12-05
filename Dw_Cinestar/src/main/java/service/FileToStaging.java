package service;

import dao.GetConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class FileToStaging {
    String url_source = null;

    public void staging() throws IOException, SQLException {
        System.out.println("start staging");
        Connection conn = null;
        PreparedStatement pre_control = null;
        String link = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\resources\\config.properties";
        try {
            InputStream input = new FileInputStream(link);
            Properties prop = new Properties();
            prop.load(input);
            // 2 Kết nối db control
            conn = new GetConnection().getConnection("db_control");
            // 3 Tìm các hàng có status P và destination là S
            ResultSet re = checkStatus(conn, "P", "S");
            // 4 Kiểm tra có tồn tại tiến trình đang chạy
            if (re.next()) {
                // 4.1 Thông báo
                System.out.println("Currently, there is another process at work."
                        // + " Use command: \n" +
                        // "\"stop\" to stop\n" +
                        // "\"stop--emergency\" for emergency stop\n" +
                        // "\"run--hands\" to run with your hands"
                );
            } else {
                // 5 Tìm các hàng có status C và destination là F
                re = checkStatus(conn, "C", "F");
                int id;
                String filename = null;
                // 6 Kiểm tra còn dòng không (Lấy giá trị từng dòng)
                while (re.next()) {
                    id = re.getInt("id");
                    filename = re.getString("filename");
                    int row_count = re.getInt("row_count");
                    String location = re.getString("source_location");
                    String path = location + "\\\\" + filename;
                    System.out.println("Doan trong phuc : "+path);
                    File file = new File(path);

//                    String absolutePath = file.getAbsolutePath().replace("\\", "/");
                    // 6. Cập nhật trạng thái status='P' và destination = S
                    String sql3 = "UPDATE data_file join data_file_configs on data_file.id_config = data_file_configs.id SET status='P', destination = 'S', data_file.update_at=now(), data_file_configs.update_at=now() WHERE data_file.id="
                            + id;
                    String sql4 = "UPDATE data_file SET status='E', "
                            + "data_file.update_at=now() WHERE id=" + id;
                    pre_control = conn.prepareStatement(sql3);
                    pre_control.executeUpdate();

                    // 6 Ktra file có tồn tại trong folder
                    System.out.println(file.getAbsolutePath());
                    System.out.println(file.exists());

                    if (!file.exists()) {
                        // file không tồn tại - cập nhật status: E - thông báo
                        pre_control = conn.prepareStatement(sql4);
                        // 8.2.1 cập nhật status: Error
                        pre_control.executeUpdate();
                        // 8.2.2 thông báo file không tồn tại
                        System.out.println(path + " does not exist");
                    } else {
                        // file tồn tại - kết nối db staging - load dữ liệu - thông báo thành công -
                        // cập nhật status: C, destination: S và status: E khi không thể load all data
                        // 8.3 kết nối db staging
                        GetConnection getConn = new GetConnection();
                        Connection conn_Staging = getConn.getConnection("db_staging");
                        // 8.3.1 Thông báo "Error connect staging"
                        if (getConn.getCheckE()) {
                            getConn.setCheckE(false);
                            pre_control.close();
                            // 8.3.2 Cập nhật trạng thái E
                            pre_control = conn.prepareStatement(sql4);
                            pre_control.executeUpdate();
                            System.exit(0);
                        }
                        int count = 0;
                        String sql = "LOAD DATA INFILE '" + path +
                                "' INTO TABLE  movie\r\n" + //
                                "FIELDS TERMINATED BY ','\r\n" + //
                                "OPTIONALLY ENCLOSED BY '\"'\r\n" + //
                                "IGNORE 1 LINES;";
                        System.out.println("LMHT : "+path);
                        PreparedStatement pre_Staging = conn_Staging.prepareStatement(sql);
                        // 8.4 import data từ file vào db staging
                        try {
                            count = pre_Staging.executeUpdate();
                        } catch (Exception e) {
                            // 8.4.1 Thông báo "The file is not in the correct format"
                            System.out.println("The file is not in the correct format");
                        }
                            // 8.5 Kiểm tra có load hết dữ liệu không
                            if (count > 0) {
                                System.out.println("Id : "+id);
                                String sql2 = "UPDATE data_file join data_file_configs on data_file.id_config = data_file_configs.id SET data_file.`status`='C', data_file_configs.destination = 'S', data_file.update_at=now() WHERE data_file.id="
                                        + id;
                                pre_control.close();
                                pre_control = conn.prepareStatement(sql2);
                                // 8.5a cập nhật status: C, destination: S
                                pre_control.executeUpdate();
                            } else {
                                String sql2 = "UPDATE data_file SET status='E', data_file.update_at=now() WHERE id="
                                        + id;
                                pre_control.close();
                                pre_control = conn.prepareStatement(sql2);
                                // 8.5b cập nhật status: E
                                pre_control.executeUpdate();
                            }
                            // 8.6 thông báo hoàn thành
                            System.out.println("Complete:\n" + "file name: " + filename
                                    + " ,total: " + count);
                        }
                    }
                }
                re.close();
                if (pre_control != null) {
                    pre_control.close();
                }
                // 9. Đóng kết nối db
                conn.close();
        } catch (Exception ex) {
        // 1.1 Thông báo không tìm thấy file
        System.out.println("Unknown file " + link);
        // 1.2 Log file
        new GetConnection().logFile("Unknown file " + link + "\n" + ex.getMessage());
        System.exit(0);
    }
        System.out.println("the end staging");

    }

    public ResultSet checkStatus(Connection conn , String status, String destination) throws Exception{
        String query = "SELECT data_file.id_config,data_file.id, data_file.filename, data_file.row_count, "
                + "data_file_configs.source_path, data_file_configs.source_location, "
                + "data_file_configs.format FROM data_file "
                + "JOIN data_file_configs ON data_file.id_config = data_file_configs.id "
                + "WHERE data_file.status = ? AND data_file_configs.destination = ? ";
        PreparedStatement preControl = conn.prepareStatement(query);
        preControl.setString(1, status);
        preControl.setString(2, destination);
        return preControl.executeQuery();
    }

    public static void main(String[] args) {
        FileToStaging staging = new FileToStaging();
        try {
            staging.staging();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
