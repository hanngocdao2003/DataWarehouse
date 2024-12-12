package service;

import dao.GetConnection;
import model.File_datas;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;

public class WarehouseToMart {

    public static void main(String[] args) throws IOException {
        // 4.1 Kết nối database
        Connection conn_control = null;
        Connection conn_mart = null;
        Connection conn_warehouse = null;

        PreparedStatement ps = null;

        try{
            conn_control = new GetConnection().getConnection("db_control");
            conn_mart = new GetConnection().getConnection("db_mart");
            conn_warehouse = new GetConnection().getConnection("db_wh");

            WarehouseToMart WTM = new WarehouseToMart();

            // 4.2 Kiểm tra có tiến trình đang chạy không : "P","M"
            if(WTM.checkProcess(conn_control, "P", "M")){
                // 4.2.1 Nếu có tiến trình đang chạy thì dừng
                System.out.println("Có tiến trình đang chạy !");

                System.exit(0);
            }else {
                // 4.2.2 Nếu không có tiến trình nào đang chạy thì tiếp tục
                System.out.println("Không có tiến trình nào đang chạy !");
                // 4.3 Lấy id của tiến trình : "C", "W"
                int idProcess = WTM.getIdDataFile(conn_control, "C", "W");
                // 4.4 Cập nhật trang thái của id đó : "P","M"
                boolean updateProcess = WTM.updateProcess(conn_control, idProcess);
                if (updateProcess) {
                    // 4.4.1 Nếu thành công thì sẽ thông báo trong file log là Cập nhật trang thái file thành công
                    System.out.println("Cập nhật trang thái file thành công");
                    // 4.5 Truncate bảng movie_aggregate trước khi add data
                    WTM.truncateTable("movie_aggregate", conn_mart);
                    // 4.6 Add data t bẳng temp_movie_result ở database warehouse
                    boolean movie_aggregate = WTM.addDataTempToAggre();
                    if (movie_aggregate) {
                        // 4.6.1 Nếu thành công hệ thống sẽ thông báo "Add data success"
                        System.out.println("Add date movie_aggre thành công");
                        // 4.7 Truncate table movie_mart trước khi add data
                        WTM.truncateTable("db_mart.movie_mart", conn_mart);
                        // 4.8 Kiểm tra add data thành công chưa
                        boolean loadToMart = WTM.loadToMart();
                        if (loadToMart) {
                            // 4.8.1 Nếu thành công hệ thống sẽ thông báo thành công
                            // 4.8.2 Xóa table temp table temp_movie_result
                            String sql = " DROP TABLE IF EXISTS db_dw.temp_movie_result";
                            ps = conn_warehouse.prepareStatement(sql);
                            ps.executeUpdate();
                            // 4.8.3 Cập nhật trang thái status và destination
                            String sqlProcess1 = "UPDATE data_file SET status='C', data_file.update_at=now() WHERE id=" + idProcess;

                            ps = conn_control.prepareStatement(sqlProcess1);
                            ps.executeUpdate();

                            // 4.8.4 Load vào data mart thành công và đóng Connection
                            System.out.println("Load data vào movie_mart thành công");
                            conn_control.close();
                            conn_warehouse.close();

                            System.out.println("Chạy thành công");
                        } else {
                            System.out.println("Load data vào movie_mart không thành công");
                            System.exit(0);
                        }
                    } else {
                        // 4.6.1 Nếu thành công hệ thống sẽ thông báo "Add data fails"
                        System.out.println("Add data failed !");
                    }
                } else {
                    // 4.4.2 Nếu thành công thì sẽ thông báo trong file log là Cập nhật trang thái file không thành công
                    System.out.println("update status and destination failed");
                }
            }
        }catch(Exception e){
            new GetConnection().logFile("Load into data mart failed");
            System.exit(0);
        }
    }


    private boolean loadToMart() throws IOException, SQLException {

        Connection conn = new GetConnection().getConnection("db_mart");
        String sql = "INSERT INTO movie_mart(date, movie_name, director, nation, genre, time_m, release_date, end_date, description, show_time, theater_name, location_name, created_at, update_at, create_by, update_by)"
                +"SELECT \n" +
                "    date_column,\n" +
                "    movie_name, \n" +
                "    director, \n" +
                "    nation, \n" +
                "    genre, \n" +
                "    time_m, \n" +
                "    release_date, \n" +
                "    end_date, \n" +
                "    description, \n" +
                "    show_time, \n" +
                "    theater_name, \n" +
                "    location_name,  \n" +
                "    created_at,  \n" +
                "    update_at, \n" +
                "    create_by, \n" +
                "    update_by\n" +
                "FROM db_dw.temp_movie_result";
        PreparedStatement ps = conn.prepareStatement(sql);
        int i = ps.executeUpdate();
        return i != 0;
    }

    private boolean addDataTempToAggre() throws IOException {
        Connection conn = new GetConnection().getConnection("db_mart");
        String sql = "INSERT INTO db_mart.movie_aggregate (date, movie_name, director, nation, genre, time_m, release_date, end_date, description, show_time, theater_name, location_name, created_at, update_at, create_by, update_by)\n" +
                "                  SELECT \n" +
                "                    date_column,\n" +
                "                    movie_name, \n" +
                "                    director, \n" +
                "                    nation, \n" +
                "                    genre, \n" +
                "                    time_m, \n" +
                "                    release_date, \n" +
                "                    end_date, \n" +
                "                    description, \n" +
                "                    show_time, \n" +
                "                    theater_name, \n" +
                "                    location_name,  \n" +
                "                    created_at,  \n" +
                "                    update_at, \n" +
                "                    create_by, \n" +
                "                    update_by \n" +
                "                    FROM db_dw.temp_movie_result";
        try(PreparedStatement ps = conn.prepareStatement(sql)){
            int update = ps.executeUpdate();
            System.out.println("transform data thành công");
            return update != 0;
        }catch (Exception e){
            System.out.println("transform data không thành công");
            e.printStackTrace();
            System.exit(0);
            return false;
        }
    }

    private void truncateTable(String tableName, Connection conn) throws IOException, SQLException {
        PreparedStatement ps = null;
        try{
            String truncateTable = "TRUNCATE TABLE "+tableName;
            ps = conn.prepareStatement(truncateTable);
            ps.executeUpdate();
            System.out.println("truncate thành công");
        }catch (Exception e){
            System.out.println("truncate thất bại");
            e.printStackTrace();
        }
    }

    private boolean updateProcess(Connection conn, int idProcess) throws SQLException {
        String updateFileConfig = "UPDATE data_file_configs SET destination = 'M', update_at = now() WHERE id = "+idProcess;
        PreparedStatement ps_dataFile = conn.prepareStatement(updateFileConfig);
        int df = ps_dataFile.executeUpdate();

        String updateDataFile = "UPDATE data_file SET status = 'P', update_at = now() WHERE id = "+idProcess;
        PreparedStatement ps_FileConfig = conn.prepareStatement(updateDataFile);
        int dfc = ps_FileConfig.executeUpdate();

        if(df != 0 && dfc != 0){
            return true;
        }
        return false;
    }

    private int getIdDataFile(Connection conn, String status, String destination) throws SQLException {
        String sql = "SELECT * FROM data_file \n" +
                "JOIN data_file_configs ON data_file.id_config = data_file_configs.id \n" +
                "WHERE data_file.status = '"+status+"' AND data_file_configs.destination = '"+destination+"'";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        int id = 0;
        while(rs.next()){
            id = rs.getInt("id");
        }
        return id;
    }

    private boolean checkProcess(Connection conn, String status, String destination) throws IOException, SQLException {
        String sql = "SELECT data_file.id, data_file.filename, data_file.row_count,"
                + "data_file_configs.source_path, data_file_configs.source_location,"
                + "data_file_configs.format,data_file_configs.colums, data_file_configs.destination "
                + "from data_file JOIN data_file_configs ON data_file.id_config = data_file_configs.id "
                + "where data_file.status='" + status + "' AND data_file_configs.destination='" + destination + "'"
                + "and data_file.update_at < CURRENT_TIMESTAMP ORDER BY data_file.update_at DESC LIMIT 2";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        if(rs.next()){
            return true;
        }
        return false;
    }
}

