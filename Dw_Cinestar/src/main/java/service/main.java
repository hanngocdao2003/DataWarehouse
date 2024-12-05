package service;

import java.io.File;

public class main {

    public static void main(String[] args) {
        String path = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\java\\crawl\\movies_data_20241202_170217.csv";
        String avbg = "D:\\DW\\DataWarehouse\\Dw_Cinestar\\src\\main\\resources\\config.properties";
        File file = new File(avbg);
        System.out.println(file.exists());
    }
}
