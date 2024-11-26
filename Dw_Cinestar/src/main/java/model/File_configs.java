package model;

import java.sql.Timestamp;

public class File_configs {
     private int id;
   private String descript;
    private String source_path;
    private String source_location;
    private String format;
    private String seperator;
    private String colums;
    private String destination;
    private Timestamp created_at;
    private Timestamp update_at;
    private String create_by;
    private String update_by;

    public File_configs() {
    }

    public File_configs(int id, String descript, String source_path, String source_location, String format, String seperator, String colums, String destination, Timestamp created_at, Timestamp update_at, String create_by, String update_by) {
        this.id = id;
        this.descript = descript;
        this.source_path = source_path;
        this.source_location = source_location;
        this.format = format;
        this.seperator = seperator;
        this.colums = colums;
        this.destination = destination;
        this.created_at = created_at;
        this.update_at = update_at;
        this.create_by = create_by;
        this.update_by = update_by;
    }

    public File_configs(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescript() {
        return descript;
    }

    public void setDescript(String descript) {
        this.descript = descript;
    }

    public String getSource_path() {
        return source_path;
    }

    public void setSource_path(String source_path) {
        this.source_path = source_path;
    }

    public String getSource_location() {
        return source_location;
    }

    public void setSource_location(String source_location) {
        this.source_location = source_location;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getSeperator() {
        return seperator;
    }

    public void setSeperator(String seperator) {
        this.seperator = seperator;
    }

    public String getColums() {
        return colums;
    }

    public void setColums(String colums) {
        this.colums = colums;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Timestamp getCreated_at() {
        return created_at;
    }

    public void setCreated_at(Timestamp created_at) {
        this.created_at = created_at;
    }

    public Timestamp getUpdate_at() {
        return update_at;
    }

    public void setUpdate_at(Timestamp update_at) {
        this.update_at = update_at;
    }

    public String getCreate_by() {
        return create_by;
    }

    public void setCreate_by(String create_by) {
        this.create_by = create_by;
    }

    public String getUpdate_by() {
        return update_by;
    }

    public void setUpdate_by(String update_by) {
        this.update_by = update_by;
    }
}
