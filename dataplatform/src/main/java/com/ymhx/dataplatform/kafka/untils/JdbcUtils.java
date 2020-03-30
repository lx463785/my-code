package com.ymhx.dataplatform.kafka.untils;

import org.springframework.stereotype.Component;

import java.beans.Transient;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class JdbcUtils {
    @Transient
    public List<Integer>   run(String sql) throws SQLException {
        Connection connection = ConnectionPool.getConnection();
//        Statement statement = connection.createStatement();
//        statement.execute(sql);
        PreparedStatement pstmt = null;
        List<Integer> list = new ArrayList<>();
        try {
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()){
                int terminalID = Integer.parseInt(rs.getString(1));
                list.add(terminalID);
            }
        }catch (SQLException e){
            try {
                connection.rollback();
            }catch (SQLException e1){
                e1.printStackTrace();
            }
        }finally {
            closeJDBC(null, pstmt, connection);
        }
        return list;

    }
    public static void closeJDBC(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取曹操专车的车辆的termid(写死了的)
     */
    public  List<Integer> getterminalID() throws SQLException {
        String sql = "SELECT ve.terminal_id from tb_vehicle ve LEFT JOIN vehicle_group gp ON ve.vehicle_group=gp.`\uFEFFID` WHERE gp.SuperiorID='100118'";
        List<Integer> terminalIds = run(sql);
        return terminalIds;
    }

}
