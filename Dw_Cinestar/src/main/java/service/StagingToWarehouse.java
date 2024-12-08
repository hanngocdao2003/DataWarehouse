package service;

import dao.GetConnection;
import model.File_datas;

import java.io.IOException;
import java.sql.*;

public class StagingToWarehouse {

    Connection warehouse = null;
    Connection conn = null;

    private boolean checkProcess(String status,String destination) throws IOException {
        conn = new GetConnection().getConnection("db_control");
        String query =  "SELECT `status`,destination FROM `data_file` \n" +
                        "JOIN data_file_configs ON data_file_configs.id = data_file.id_config \n" +
                        "WHERE `status`=? AND destination=?";
        try {
            // Thực hiện truy vấn để lấy thông tin từ data_file_configs
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, destination);
            ResultSet resultSet = preparedStatement.executeQuery();
            // Kiểm tra xem có bản ghi nào trả về hay không
            if (resultSet.next()) {
                // Có bản ghi, trả về true
                return true;
            } else {
                // Không có bản ghi, trả về false
                return false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  false;
    }

    private void updateStatus(int dataFileId, String newStatus,String note) throws SQLException, IOException {
        conn = new GetConnection().getConnection("db_control");
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?,note =? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }



    private File_datas getProcessWH(String status,String destination) throws SQLException, IOException {
        conn = new GetConnection().getConnection("db_control");
        File_datas dataFile = null;
//        DataFileConfig dataFileConfig = null;

        String query =  "SELECT data_file.*,data_file_configs.* FROM `data_file` \n" +
                        "JOIN data_file_configs ON data_file_configs.id = data_file.id_config\n" +
                        "WHERE `status`=? AND destination=?";

        // Thực hiện truy vấn để lấy thông tin từ data_file_configs
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        preparedStatement.setString(1, status);
        preparedStatement.setString(2, destination);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {

            // Tạo đối tượng model.DataFileConfig và gán giá trị từ ResultSet
            dataFile = new File_datas();
            dataFile.setId(resultSet.getLong("id"));
            dataFile.setDfConfigId(resultSet.getInt("id_config"));
            dataFile.setName(resultSet.getString("filename"));
            dataFile.setRowCount(resultSet.getInt("row_count"));
            dataFile.setStatus(resultSet.getString("status"));
            dataFile.setNote(resultSet.getString("note"));
            System.out.println("Co du liệu id config");

        }
        else {
            // 4.1 Thông báo lỗi
            new GetConnection().logFile("Lấy thông tin thất bại");
            System.exit(0);
        }
        return  dataFile;
    }

    private void updateStatusConfig(int dataFileConfigId, String des) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file_configs SET destination = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, des);
            updateStatement.setLong(2, dataFileConfigId);
            updateStatement.executeUpdate();
        }
    }

    public void truncateTable(Connection conn, String tableName) {
        try (Statement statement = conn.createStatement()) {
            String truncateQuery = "TRUNCATE TABLE " + tableName;
            statement.executeUpdate(truncateQuery);
            System.out.println("Đã truncate bảng " + tableName);
        } catch (SQLException e) {
            System.out.println("Lỗi khi thực hiện TRUNCATE TABLE: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    public boolean transformDataTheaterDim() throws IOException {
        Connection stagging = new GetConnection().getConnection("db_control");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.currency_dim (id_bank, currency_code, currency_name, dt_expired)\n" +
                "SELECT\n" +
                "    bd.id_bank,\n" +
                "    er.currency_code,\n" +
                "    er.currency_name,\n" +
                "    '2030-01-01' as dt_expired\n" +
                "FROM staging.exchange_rate er\n" +
                "JOIN data_warehouse.bank_dim bd ON er.bank_name = bd.bank_name";

        //7.2.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }

    }

    public boolean transformDataDateDim() throws IOException {
        Connection staging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO date_dim (id_date, date, day, month, year, hour, minute)\n" +
                "SELECT \n" +
                "    ROW_NUMBER() OVER (ORDER BY m.Date) AS id_date,\n" +
                "    m.Date AS date,\n" +
                "    DAYNAME(m.Date) AS day,\n" +
                "    MONTHNAME(m.Date) AS month,\n" +
                "    YEAR(m.Date) AS year,\n" +
                "    HOUR(m.Date) AS hour,\n" +
                "    MINUTE(m.Date) AS minute\n" +
                "FROM \n" +
                "    (SELECT DISTINCT Date FROM db_staging.movie WHERE Date IS NOT NULL) AS m;";

        // 7.3.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformDataMovieDim() throws IOException {
        Connection staging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformMovieDimQuery = "INSERT INTO db_dw.movie_dim (movie_name, nation, genre, director, actor, time_m, description, expiration_date)\n" +
                "SELECT DISTINCT movie_name, \n" +
                "                  nation, \n" +
                "                  genre, \n" +
                "                  director, \n" +
                "                  actor,\n" +
                "                  time_m, \n" +
                "                  description, \n" +
                "                  '2030-01-01' AS expiration_date \n" +
                "FROM db_staging.movie";

        // 7.1.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformMovieDimQuery)) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Thành công");

            // Trả về true nếu có ít nhất một hàng bị ảnh hưởng (được chèn)
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformScheduleDim() throws IOException {
        Connection staging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformMovieDimQuery = "INSERT INTO schedule_dim (release_date, end_date, show_time, expiration_date)\n" +
                "SELECT release_date,\n" +
                "       end_date,\n" +
                "       show_time,\n" +
                "       '2030-01-01' AS expiration_date \n" +
                "FROM db_staging.movie\n" +
                "WHERE release_date IS NOT NULL \n" +
                "AND show_time IS NOT NULL\n" +
                "GROUP BY show_time";

        // 7.1.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformMovieDimQuery)) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Thành công");

            // Trả về true nếu có ít nhất một hàng bị ảnh hưởng (được chèn)
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformFilmFact() throws IOException {
        Connection staging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformMovieDimQuery = "INSERT INTO film_fact (id_theater, id_schedule, id_movie, expiration_date)\n" +
                "SELECT td.id, \n" +
                "       sd.id, \n" +
                "       md.id, \n" +
                "       '2030-01-01' AS expiration_date FROM db_staging.movie m\n" +
                "JOIN db_dw.movie_dim md ON md.movie_name = m.movie_name\n" +
                "JOIN db_dw.theater_dim td ON td.theater_name = m.theater_name\n" +
                "JOIN db_dw.schedule_dim sd ON sd.show_time = m.show_time";

        // 7.1.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformMovieDimQuery)) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Thành công");

            // Trả về true nếu có ít nhất một hàng bị ảnh hưởng (được chèn)
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformTheaterDim() throws IOException {
        Connection staging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformMovieDimQuery = "INSERT INTO theater_dim (theater_name, location_name) \n" +
                "SELECT DISTINCT theater_name, \n" +
                "                location_name\n" +
                "FROM db_staging.movie \n" +
                "WHERE theater_name IS NOT NULL \n" +
                "AND location_name IS NOT NULL;";

        // 7.1.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformMovieDimQuery)) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Thành công");

            // Trả về true nếu có ít nhất một hàng bị ảnh hưởng (được chèn)
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformDataAggre() throws IOException{
        Connection stagging = null;
        Connection warehouse = null;

        try {
            stagging = new GetConnection().getConnection("db_staging");
            warehouse = new GetConnection().getConnection("db_wh");

            // Transform dữ liệu vào bảng bank_dim
            String transformBankDimQuery = "INSERT INTO movie_aggregate \n" +
                    "(date, movie_name, director, nation, genre, time_m, release_date, end_date, description, show_time, theater_name, location_name, created_at, update_at, create_by, update_by)\n" +
                    "SELECT \n" +
                    "    dd.Date,\n" +
                    "    md.movie_name, \n" +
                    "    md.director, \n" +
                    "    md.nation, \n" +
                    "    md.genre, \n" +
                    "    md.time_m, \n" +
                    "    sd.release_date, \n" +
                    "    sd.end_date, \n" +
                    "    md.description, \n" +
                    "    sd.show_time, \n" +
                    "    td.theater_name, \n" +
                    "    td.location_name,  \n" +
                    "    NOW() as created_at,  \n" +
                    "    NOW() as update_at, \n" +
                    "    'Phuc' as create_by, \n" +
                    "    'Phuc' as update_by\n" +
                    "FROM db_staging.movie m\n" +
                    "JOIN db_dw.date_dim dd ON m.Date = dd.date\n" +
                    "JOIN db_dw.movie_dim md ON md.movie_name = m.movie_name\n" +
                    "JOIN db_dw.theater_dim td ON td.theater_name = m.theater_name\n" +
                    "JOIN db_dw.schedule_dim sd ON sd.show_time = m.show_time;";

            // Kiểm tra xem việc transform có thành công không
            try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println("Thành công");
                return rowsAffected > 0;
            }
        } catch (IOException e) {
            System.out.println("Lỗi IOException: " + e.getMessage());
            new GetConnection().logFile("Transform thất bại do lỗi IO");
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại do lỗi SQL");
        } finally {
            // Đóng kết nối để tránh rò rỉ tài nguyên
            try {
                if (stagging != null) stagging.close();
                if (warehouse != null) warehouse.close();
            } catch (SQLException e) {
                System.out.println("Lỗi khi đóng kết nối: " + e.getMessage());
            }
        }
        return false;
    }


    public boolean transdormDataAggreAvg() throws IOException {
        Connection stagging = new GetConnection().getConnection("db_staging");
        warehouse = new GetConnection().getConnection("db_wh");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery =  "INSERT INTO avg_movie_aggregate \n" +
                "(month_avg, year_avg, movie_name, director, nation, genre, time_m, release_date, end_date, description, show_time, number_of_showtimes, theater_name, location_name, created_at, update_at, create_by, update_by)\n" +
                "SELECT \n" +
                "    MONTHNAME(m.Date) AS month_avg,  -- assuming you want to extract month\n" +
                "    YEAR(m.Date) AS year_avg,    -- assuming you want to extract year\n" +
                "    md.movie_name, \n" +
                "    md.director, \n" +
                "    md.nation, \n" +
                "    md.genre, \n" +
                "    md.time_m, \n" +
                "    sd.release_date, \n" +
                "    sd.end_date, \n" +
                "    md.description, \n" +
                "    sd.show_time, \n" +
                "    count(*) AS number_of_showtimes,\n" +
                "    td.theater_name, \n" +
                "    td.location_name,  \n" +
                "    NOW() AS created_at,  \n" +
                "    NOW() AS update_at, \n" +
                "    'Phuc' AS create_by, \n" +
                "    'Phuc' AS update_by\n" +
                "FROM db_staging.movie m\n" +
                "JOIN db_dw.movie_dim md ON md.movie_name = m.movie_name\n" +
                "JOIN db_dw.theater_dim td ON td.theater_name = m.theater_name\n" +
                "JOIN db_dw.schedule_dim sd ON sd.show_time = m.show_time\n" +
                "GROUP BY \n" +
                "    MONTH(m.Date),\n" +
                "    YEAR(m.Date),\n" +
                "    md.movie_name";

        // 7.6.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        Connection staging = new GetConnection().getConnection("db_staging");
        Connection warehouse = new GetConnection().getConnection("db_wh");

        StagingToWarehouse STW = new StagingToWarehouse();

        if(STW.checkProcess("P","W")){
            System.out.println("Co tien trinh dang chay");
            // 3.1 Thông báo lỗi
            new GetConnection().logFile("Co tien trinh dang chay");
            System.exit(0);
        }
        else {
            System.out.println("Khong co tien trinh dang chay");
        }

        // 4.lấy thông tin của  tiến trình transform chưa chạy
        System.out.println(STW.getProcessWH("C","S"));
        File_datas dataFile = STW.getProcessWH("C","S");
        // 5. Cập nhật  trạng thái để chạy tiến trình
        STW.updateStatus((int) dataFile.getId(),"P","Process data transform");
        STW.updateStatusConfig(dataFile.getDfConfigId(),"W");

        // 6. Tiến hành truncate các bảng ở warehouse
        STW.truncateTable(warehouse,"movie_dim");
        STW.truncateTable(warehouse,"theater_dim");
        STW.truncateTable(warehouse,"schedule_dim");
        STW.truncateTable(warehouse,"date_dim");
        STW.truncateTable(warehouse,"film_fact");
        STW.truncateTable(warehouse,"movie_aggregate");
        STW.truncateTable(warehouse,"avg_movie_aggregate");

        // 7. Tiến hành transform dữ liệu
        // 7.1 transform bảng bank_dim
        STW.transformDataMovieDim();
        STW.transformDataDateDim();
        STW.transformScheduleDim();
        STW.transformTheaterDim();
        STW.transformFilmFact();
        STW.transformDataAggre();
        STW.transdormDataAggreAvg();

        STW.updateStatus((int) dataFile.getId(),"C","Transform data succesfull");
        STW.truncateTable(staging,"movie");
    }
}
