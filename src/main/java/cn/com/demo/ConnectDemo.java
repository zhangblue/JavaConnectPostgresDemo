package cn.com.demo;

import cn.com.database.DBBridge;

import java.sql.SQLException;

public class ConnectDemo {

    public static void main(String[] args) {
        String strSql = "select * from users";

        DBBridge db = new DBBridge("dbname","pg_user","pg_pass");
        try {
            db.openBridge();
            db.execSELECT(strSql);
            while (db.nextRow()) {
                db.getString("name");
                db.getInt("age");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                db.closeBridge();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
