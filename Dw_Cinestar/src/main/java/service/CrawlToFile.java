package service;

//import dao.DatabaseConnection;
import dao.GetConnection;
import model.File_configs;
import model.File_datas;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class CrawlToFile {

    public boolean checkProcess(Connection conn, String p, String s) {
        String sql = "SELECT `status`,destination FROM `data_file` \n" +
                "JOIN data_file_configs ON data_file_configs.id = data_file.id_config\n" +
                "WHERE `status`=? AND destination=?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, p);
            ps.setString(2, s);
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                new GetConnection().logFile("Checking Process");
                return true;
            }else{
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int loadIdDataFileConfig(Connection conn){
        int idDataFileConfig = 0;
        CrawlToFile ctf = new CrawlToFile();
        String config = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\resources\\config.properties";
        try {
            // 1.3 kết nối configuration.properties
            InputStream is = new FileInputStream(config);
            // 1.4 Load source configuration
            Properties prop = new Properties();
            prop.load(is);
            // 1.5 lấy dữ liệu từ source để thêm vào data_file_config
            String folderLocation = prop.getProperty("folder_location");

            // 1.6 Thêm dữ liệu vào table data_file_config
            idDataFileConfig = ctf.addDataFileConfig(conn, folderLocation);
            return idDataFileConfig;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int addDataFileConfig(Connection conn, String folderLocation){
        String sql = "INSERT INTO data_file_configs (descript, source_location, format, `seperator`, colums, destination, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        long idDataFileConfig = -1;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            String location = folderLocation;
            java.util.Date createdAt = new Date(); // Lấy ngày hiện tại
            Timestamp updatedAt = null; // Chưa có thông tin về ngày cập nhật
            String createdBy = "Phúc";
            String updatedBy = null; // Chưa có thông tin về người cập nhật

            // Thay đổi dữ liệu này dựa trên cấu trúc bảng và cột thực tế của bạn
            preparedStatement.setString(1, "nguồn lấy dữ liệu"); // description
            preparedStatement.setString(2,location ); // location
            preparedStatement.setString(3, ".csv"); // format
            preparedStatement.setString(4, ","); // separator
            preparedStatement.setString(5, "700"); // columns
            preparedStatement.setString(6, "F"); // destination
            preparedStatement.setTimestamp(7, new Timestamp(createdAt.getTime()));
            preparedStatement.setTimestamp(8,  null);
            preparedStatement.setString(9, createdBy); // created_by
            preparedStatement.setString(10, updatedBy); // updated_by

            int rs = preparedStatement.executeUpdate();
            if(rs > 0){
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        idDataFileConfig = generatedKeys.getInt(1);
//                        System.out.println(idDataFileConfig);
                    }
                }
            }

            return (int) idDataFileConfig;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public File_configs check(Connection conn ,int idDataFileConfig){
        File_configs dataFileConfig = null;

        String sql = "SELECT data_file_configs.id, data_file_configs.source_location,data_file_configs.format,data_file_configs.colums, data_file_configs.destination from data_file_configs " +
                "WHERE data_file_configs.id = ?";
        try {
            // Thực hiện truy vấn để lấy thông tin từ data_file_configs
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setInt(1, idDataFileConfig);
            ResultSet resultSet = preparedStatement.executeQuery();

            // Kiểm tra xem có dữ liệu trả về không
            if (resultSet.next()) {
                dataFileConfig = new File_configs();
                dataFileConfig.setId((int) resultSet.getLong("id"));
                dataFileConfig.setLocation(resultSet.getString("source_location"));
                dataFileConfig.setFormat(resultSet.getString("format"));
                dataFileConfig.setColumns(resultSet.getString("colums"));
                dataFileConfig.setDestination(resultSet.getString("destination"));
//                System.out.println("Co du liệu id config");
            } else {
                new GetConnection().logFile("Không tìm thấy dữ liệu cho idFileConfig:");
                System.out.println("Không tìm thấy dữ liệu cho idFileConfig: " + idDataFileConfig);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dataFileConfig;
    }

    public boolean runScript() throws IOException {
        String path = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\java\\crawl\\crawl1.py";
        RunPythonScript runScriptPython = new RunPythonScript();

        if(runScriptPython.runScript(path) != null) {
            // 1.7.1 nếu thành công thì sẽ trả về tên file .csv và cập nhật logFile load thành  công
            System.out.println("Crawl file success");
            new GetConnection().logFile("Chạy script data thành công");
            return true;
        } else {
            // 1.7.2 nếu không thành công thì sẽ trả về tên file .csv và cập nhật logFile load không thành  công
            System.out.println("Chạy script data không thành công");
            new GetConnection().logFile("Chạy script data không thành công");
            return false;
        }
    }
    public File_datas addDataFile( Connection conn,int dfConfigId) throws SQLException, IOException {
        String insertSql = "INSERT INTO data_file (id_config, filename, row_count, status, note, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String currentDateTime = dateFormat.format(new Date());
        // Use the currentDateTime to create the file name
        String csvFileName = "movies_data_"+ currentDateTime + ".csv";

        try (PreparedStatement preparedStatement = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            int dfConfig = dfConfigId;
            String name = csvFileName;
            int rowCount = 14;
            String status = "N";
            String note = "Data imported successfully";
            Date createdAt = new Date();
            Timestamp updatedAt = null;
            String createdBy = "Phúc";
            String updatedBy = null;

            preparedStatement.setLong(1, dfConfig);
            preparedStatement.setString(2, name);
            preparedStatement.setInt(3, rowCount);
            preparedStatement.setString(4, status);
            preparedStatement.setString(5, note);
            preparedStatement.setTimestamp(6, new Timestamp(createdAt.getTime()));
            preparedStatement.setTimestamp(7, new Timestamp(createdAt.getTime()));
            preparedStatement.setString(8, createdBy);
            preparedStatement.setString(9, updatedBy);

            preparedStatement.executeUpdate();

            // Retrieve the generated keys (including the ID of the inserted row)
            ResultSet resultSet = preparedStatement.getGeneratedKeys();

            if (resultSet.next()) {
                long generatedId = resultSet.getLong(1);

                // Create a DataFile object with the inserted data
                File_datas dataFile = new File_datas();
                dataFile.setId(generatedId);
                dataFile.setDfConfigId(dfConfigId);
                dataFile.setName(name);
                dataFile.setRowCount(rowCount);
                dataFile.setStatus(status);
                dataFile.setNote(note);

                return dataFile;
            } else {
                // Handle the case where no generated keys are available
                return null;
            }
        }
    }

    private void updateStatus(Connection conn, int dataFileId, String newStatus,String note) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?,note =? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }

    private void updateFileName(Connection conn, int dataFileId,String actualFileName) throws SQLException {
        String updateStatusAndFileNameSql = "UPDATE data_file SET name = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndFileNameSql)) {
            updateStatement.setString(1, actualFileName);
            updateStatement.setLong(2, dataFileId);
            updateStatement.executeUpdate();
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        // 1.1 Kết noi database
        Connection conn = new GetConnection().getConnection("db_control");
        CrawlToFile CTF = new CrawlToFile();

        // 1.2 Kiểm tra có tiến trình đang chay không
        if (CTF.checkProcess(conn, "P", "F")) {
            // 1.2.1 Nếu có thì ngừng tiến trình
            System.out.println("Another process is running");
            System.exit(0);
        } else {
            // 1.2.2 Nếu khng th tiếp tục chạy
            System.out.println("Have no process is running");
        }

        // **************************
        int idDataFileConfig = CTF.loadIdDataFileConfig(conn);

        // 1.6.1 Kiểm tra xem đã thêm dữ liệu thành công chưa
        File_configs dfFileConfig = CTF.check(conn, idDataFileConfig);

        // 1.7 Run Script crawl data
        boolean runScriptPython;
        try {
            runScriptPython = CTF.runScript();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File_datas dataFile = null;

        try{
            // 1.9. thêm value mới vào file_datas dựa vào id của file_configs
            dataFile = CTF.addDataFile(conn, idDataFileConfig);
            System.out.println(dataFile);
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }

        try {
            // 1.10. cập nhật status trong file_datas sang P và Note là Data import success
            CTF.updateStatus(conn, (int) dfFileConfig.getId(), "P", "Data import process");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        // 1.11 Kiểm tra crawl có thành công không
        if (runScriptPython) {
            System.out.println("Crawl operation successful.");
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                String currentDateTime = dateFormat.format(new Date());

                String csvFileName = "movies_data_" + currentDateTime + ".csv";
                // 1.12.1 cập nhật status trong file_datas sang C
                System.out.println("doan trong phuc"+csvFileName);
                // 1.12.2 Cập nhật lại tên file
                CTF.updateStatus(conn, (int) dfFileConfig.getId(), "C", "Data import success");
                CTF.updateFileName(conn, (int) dfFileConfig.getId(), csvFileName);
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        } else {
            System.out.println("Crawl operation failed.");
            try {
                // 1.11.2 ghi log vào D:\DataWarehouse\file\logs\file_logs.txt
                new GetConnection().logFile("Crawl operation failed.");
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
            try {
                // 1.11.3 cập nhật status trong file_datas sang E
                CTF.updateStatus(conn, (int) dfFileConfig.getId(), "E", "Data import error");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
