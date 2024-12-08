package service;

import dao.GetConnection2;
import model.File_datas;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;

public class WarehouseToMart {

    Connection conn = null;
    static Timestamp date = new Timestamp(System.currentTimeMillis());
    static String date_err = date.toString();
    static String fileName = "D:\\LogERR" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";

    private ResultSet checkProcess(Connection conn, PreparedStatement ps, String status, String destination) throws IOException, SQLException {
        conn = new GetConnection2().getConnection("db_control");
        String sql = "SELECT data_file.id, data_file.name, data_file.row_count,"
                + "data_file_configs.source_path, data_file_configs.location,"
                + "data_file_configs.format,data_file_configs.colums, data_file_configs.destination "
                + "from data_file JOIN data_file_configs ON data_file.df_config_id = data_file_configs.id "
                + "where data_file.status='" + status + "' AND data_file_configs.destination='" + destination + "'"
                + "and data_file.update_at < CURRENT_TIMESTAMP ORDER BY data_file.update_at DESC LIMIT 2";
        ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        return rs;
    }

    private void updateErrConnect(PreparedStatement psControl, Connection connControl, ArrayList<Integer> idArr, String mess) throws IOException {
        try {
            if (GetConnection2.checkE == 0) {
                for (Integer idE : idArr) {
                    String sqlE = "UPDATE data_file SET status='E', data_file.update_at=now() WHERE id=" + idE;
                    psControl = connControl.prepareStatement(sqlE);
                    psControl.executeUpdate();
                }
                System.out.println("Kết nối đến " + mess + " không thành công");
                System.exit(0);
            }
        } catch (Exception e) {
            PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
            writer.println("Error: " + e.getMessage());
            e.printStackTrace(writer);
            writer.close();
            System.exit(0);
            // TODO Auto-generated catch block
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        Connection conn_control = null;
        PreparedStatement ps_control = null;
        WarehouseToMart WTM = new WarehouseToMart();

        conn_control = new GetConnection2().getConnection("db_control");

        ResultSet rs1 = WTM.checkProcess(conn_control, ps_control, "P", "M");
        if(rs1.next()){
            System.out.println("Có tiến trình đang chạy trong hệ thống");
        }else {
            ResultSet rs2 = WTM.checkProcess(conn_control, ps_control, "C", "W");
            if(!rs2.next()){
                System.out.println("Không có data ở warehouse");
            }else{
                ArrayList<Integer> idArr = new ArrayList<Integer>();
                while(rs2.next()){
                    int id = rs2.getInt("id");
                    idArr.add(id);

                    String sql1 = "UPDATE data_file SET status = 'P', update_at = now() WHERE id = "+id;
                    ps_control = conn_control.prepareStatement(sql1);
                    ps_control.executeUpdate();

                    String sql2 = "UPDATE data_file_configs SET destination = 'M', update_at = now() WHERE id = "+id;
                    ps_control = conn_control.prepareStatement(sql2);
                    ps_control.executeUpdate();
                }

                Connection conn_wh = new GetConnection2().getConnection("db_wh");
                WTM.updateErrConnect(ps_control, conn_control, idArr, "warehouse");

                Connection conn_mart = new GetConnection2().getConnection("db_mart");
//			10.1  Tạo file ghi lỗi và update status ="E" của Control.data_file tại các dòng tìm được ở mục 6
                WTM.updateErrConnect(ps_control, conn_control, idArr, "mart");

//		11. Sử dụng db mart
                String useDatabaseSQL = "USE mart";
                Statement useDatabaseStatement = conn_mart.createStatement();
                useDatabaseStatement.execute(useDatabaseSQL);

                String truncateTabledb_rename_avg = "TRUNCATE TABLE db_rename_avg";
                String truncateTabledb_rename_exchange = "TRUNCATE TABLE db_rename_movie";
                Statement statement = conn_mart.createStatement();
                statement.executeUpdate(truncateTabledb_rename_avg);
                statement.executeUpdate(truncateTabledb_rename_exchange);
                System.out.println("Bảng truncateTable db_rename_exchange đã được truncate thành công.");
                System.out.println("Bảng truncateTable db_rename_avg đã được truncate thành công.");


            }
        }
    }




}
