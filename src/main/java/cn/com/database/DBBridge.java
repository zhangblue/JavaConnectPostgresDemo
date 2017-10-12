// ********************************************************************************//
// File Name : DBBridge.java //
// Author : clm //
// Created time : 2004-10-8 //
// Description : Get real time statistics from servlet and display through
// applet//
// //
// //
// Copyright (c) 1999-2004 ATM R&D Center, BUPT. All Rights Reserved. //
// ********************************************************************************//
package cn.com.database;

import java.math.BigDecimal;
import java.sql.*;


public class DBBridge {
    private Connection conn = null;
    private ResultSet rs = null;
    private Statement stmt = null;
    private PreparedStatement pstmt = null;
    private String db_url = "";
    private String db_name = "";
    private String pg_user = "";
    private String pg_pass = "";

    public DBBridge(String db_name, String pg_user, String pg_pass) {
        this.db_name = db_name;
        this.pg_user = pg_user;
        this.pg_pass = pg_pass;
    }


    private void clearResult() throws SQLException {
        try {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (pstmt != null)
                pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        rs = null;
        stmt = null;
        pstmt = null;
    }

    public ResultSet execSELECT(String sqls) throws SQLException {

        if (conn == null)
            throw new SQLException("连接还没有被建立!");
        if (sqls == null)
            throw new SQLException("SQL-statement是null!");
        clearResult();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqls);
        } catch (SQLException e) {
            e.printStackTrace();
            if (e.toString().toLowerCase().indexOf("unknown response type") != -1) {
                // connectionPool.reTryConnection(conn);
                throw (new SQLException("数据库连接错误,请稍候重试!"));
            } else {
                throw e;
            }
        }
        return rs;
    }

    /**
     * 执行批处理,lxw,2005-10-27
     *
     * @param sqls String
     * @return int[]
     * @throws SQLException
     */
    public int[] executeBatUpdate(String[] sqls) throws SQLException {
        if (conn == null)
            throw new SQLException("连接还没有被建立!");
        if (sqls == null)
            throw new SQLException("SQL-statement是null!");
        clearResult();
        conn.setAutoCommit(true);
        stmt = conn.createStatement();
        for (int i = 0; i < sqls.length; i++) {
            stmt.addBatch(sqls[i]);
        }
        int[] numRow = stmt.executeBatch();
        return numRow;
    }

    /**
     * 执行更新,lxw,2005-4-15
     *
     * @param sqls String
     * @return int 经测试发现经此方法可以正确执行中文的插入和修改,而本类中的其他方法则有可能不正常。
     * @throws SQLException
     */
    public int executeUpdate(String sqls) throws SQLException {
        if (conn == null)
            throw new SQLException("连接还没有被建立!");
        if (sqls == null)
            throw new SQLException("SQL-statement是null!");
        clearResult();
        conn.setAutoCommit(true);
        stmt = conn.createStatement();
        if (sqls.contains("copy ")) {
            stmt.executeUpdate("SET client_encoding TO 'SQL_ASCII'");
        }
        int numRow = stmt.executeUpdate(sqls);
        return numRow;
    }

    public int execSQL(String sqls, String args[]) throws SQLException {
        if (conn == null)
            throw new SQLException("连接还没有被建立!");
        if (sqls == null)
            throw new SQLException("SQL-statement是null!");
        clearResult();
        conn.setAutoCommit(true);
        pstmt = conn.prepareStatement(sqls);
        if (args != null)
            for (int i = 0; i < args.length; i++) {
                int len = 0;
                String addStr = "";
                for (int j = 0; j < args[i].length(); j++) {
                    if (args[i].charAt(j) > 0x7f)
                        len++;
                }
                for (int j = 0; j < len; j += 2)
                    addStr += " ";
                String temp = null;
                try {
                    temp = new String((args[i] + addStr).getBytes("ISO8859_1"), "gb2312");
                } catch (Exception e) {
                    e.printStackTrace();
                    temp = args[i] + addStr;
                }
                pstmt.setString(i + 1, temp);
            }
        int numRow = pstmt.executeUpdate();
        return numRow;
    }

    public boolean nextRow() throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.next();
    }

    public String getString(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getString(fieldName);
    }

    public int getInt(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getInt(fieldName);
    }

    public long getLong(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getLong(fieldName);
    }

    public float getFloat(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getFloat(fieldName);
    }

    public BigDecimal getBigDecimal(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getBigDecimal(fieldName);
    }

    public double getDouble(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getDouble(fieldName);
    }

    public Timestamp getTimestamp(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getTimestamp(fieldName);
    }

    public boolean getBoolean(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getBoolean(fieldName);
    }

    public byte getByte(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null(getByte)!");
        return rs.getByte(fieldName);
    }

    public byte[] getBytes(String fieldName) throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null!");
        return rs.getBytes(fieldName);
    }

    public void openBridge() throws SQLException {
        this.db_url = "127.0.0.1";
        this.openBridge(db_url);
    }

    public void openBridge(String db_url) throws SQLException {
        clearResult();
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:postgresql://" + db_url + ":5432/" + db_name + "?user=" + pg_user + "&password=" + pg_pass + "&loginTimeout=5");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeBridge() throws SQLException {
        clearResult();
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    public int getRow() throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null(getRow)!");
        return rs.getRow();
    }

    public void beforeFirst() throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null(beforeFirst)!");
        rs.beforeFirst();
    }

    public void afterLast() throws SQLException {
        if (rs == null)
            throw new SQLException("ResultSet是null(afterLast)!");
        rs.afterLast();
    }
}
