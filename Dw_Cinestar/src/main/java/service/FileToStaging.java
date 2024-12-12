package service;

import dao.GetConnection;
import log.MailTo;

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
            //2.1 Đọc tệp config.properties để lấy thông tin cấu hình
            InputStream input = new FileInputStream(link);
            Properties prop = new Properties();
            prop.load(input);
            // 2.2 Kết nối db control để thực hiện truy vấn kiểm tra trạng thái.
            conn = new GetConnection().getConnection("db_control");
            // 2.3.1 Tìm các hàng có status P và destination là S
            ResultSet re = checkStatus(conn, "P", "S");
            // 2.3 Kiểm tra có tồn tại tiến trình đang chạy
            if (re.next()) {
                // 2.4.1 Thông báo có và dừng quá trình
                System.out.println("Currently, there is another process at work."
                );
            } else {
                // 2.5 Tìm các hàng có status C và destination là F
                re = checkStatus(conn, "C", "F");
                int id;
                String filename = null;
                // 2.4,2.6  Kiểm tra còn dòng không (Lấy giá trị từng dòng)
                while (re.next()) {
                    //lấy trong data file, data file config
                    id = re.getInt("id");
                    filename = re.getString("filename");
                    int row_count = re.getInt("row_count");
                    String location = re.getString("source_location");
                    String path = location + "\\\\" + filename;
                    System.out.println("Path: " + path);
                    File file = new File(path);

//                    String absolutePath = file.getAbsolutePath().replace("\\", "/");
                    // 2.8 Cập nhật trạng thái status='P' và destination = S
                    String sql3 = "UPDATE data_file join data_file_configs on data_file.id_config = data_file_configs.id SET status='P', destination = 'S', data_file.update_at=now(), data_file_configs.update_at=now() WHERE data_file.id="
                            + id;
                    String sql4 = "UPDATE data_file SET status='E', "
                            + "data_file.update_at=now() WHERE id=" + id;
                    pre_control = conn.prepareStatement(sql3);
                    pre_control.executeUpdate();

                    // 2.7 Ktra file có tồn tại trong folder
                    System.out.println(file.getAbsolutePath());
                    System.out.println(file.exists());


                    //2.7.1 file không tồn tại -- cập nhật status: E - thông báo
                    if (!file.exists()) {
                        pre_control = conn.prepareStatement(sql4);

                        pre_control.executeUpdate();
                        //2.7.2 Thông báo "does not exist"
                        System.out.println(path + " does not exist");
                    } else {
                        // file tồn tại
                        // file tồn tại - kết nối db staging - load dữ liệu - thông báo thành công -
                        // cập nhật status: C, destination: S và status: E khi không thể load all data

                        // 2.9 Kết nối db_staging để tải dữ liệu từ tệp vào bảng staging
                        GetConnection getConn = new GetConnection();
                        Connection conn_Staging = getConn.getConnection("db_staging");
                        // 2.9.1 Thông báo
                        if (getConn.getCheckE()) {
                            getConn.setCheckE(false);
                            pre_control.close();
                            // 2.9.2 Cập nhật status: E
                            pre_control = conn.prepareStatement(sql4);
                            pre_control.executeUpdate();
                            System.exit(0);
                        }
                        int count = 0;

                        // 2.10 import data từ file vào db staging, Thực hiện lệnh LOAD DATA INFILE
                        String sql = "LOAD DATA INFILE '" + path +
                                "' INTO TABLE  db_staging.movie\r\n" + //
                                "FIELDS TERMINATED BY ','\r\n" + //
                                "OPTIONALLY ENCLOSED BY '\"'\r\n" + //
                                "IGNORE 1 LINES"+ "(movie_name, director, actor, nation, genre, time_m, @release_date, @end_date, description, \n" +
                                "@show_time, theater_name, location_name, @date)";
                        System.out.println("LMHT : "+ path);
                        PreparedStatement pre_Staging = conn_Staging.prepareStatement(sql);

                        try {
                            count = pre_Staging.executeUpdate();
                        } catch (Exception e) {
                            // 2.10.1 Thông báo
                            System.out.println(e.getMessage());
                            System.out.println("The file is not in the correct format");
                        }
                        // 2.11 Kiểm tra có load hết dữ liệu không
                        if (count > 0) {
                            System.out.println("Id : "+id);
                            String sql2 = "UPDATE data_file join data_file_configs on data_file.id_config = data_file_configs.id SET data_file.`status`='C', data_file_configs.destination = 'S', data_file.update_at=now() WHERE data_file.id="
                                    + id;
                            pre_control.close();
                            pre_control = conn.prepareStatement(sql2);
                            //2.11.1  cập nhật status: C, destination: S (nếu thành công)
                            pre_control.executeUpdate();
                        } else {
                            String sql2 = "UPDATE data_file SET status='E', data_file.update_at=now() WHERE id="
                                    + id;
                            pre_control.close();
                            pre_control = conn.prepareStatement(sql2);
                            // 2.11.2 cập nhật status: E (nếu thất bại)
                            pre_control.executeUpdate();
                        }
                        System.out.println("Complete:\n" + "file name: " + filename
                                + " ,total: " + count);
                    }
                }
            }
            //2.4.2 Đóng kết nối db
            re.close();
            if (pre_control != null) {
                pre_control.close();
            }
            // Đóng kết nối db
            conn.close();
        } catch (Exception ex) {
            // 2.1.1 Thông báo không tìm thấy file
            System.out.println("Unknown file " + link);
            // 2.1.2 Log file
            GetConnection con = new GetConnection();
            con.logFile("Unknown file " + link + "\n" + ex.getMessage());
            MailTo.sendVerificationEmail(con.logFileToMail("Unknown file " + link + "\n" + ex.getMessage()));
            System.exit(0);
        }
        // 2.12 thông báo hoàn thành
        System.out.println("the end staging");

    }
    // Hàm kiểm tra trạng thái tiến trình
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
