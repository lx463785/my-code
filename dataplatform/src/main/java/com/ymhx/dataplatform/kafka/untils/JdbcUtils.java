package com.ymhx.dataplatform.kafka.untils;

import org.springframework.stereotype.Component;

import java.beans.Transient;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class JdbcUtils {
    @Transient
    public List<String>   run(String sql) throws SQLException {
        Connection connection = JdbcPoolUtils.getConnection();
        PreparedStatement pstmt = null;
        List<String> list = new ArrayList<>();
        ResultSet rs=null;
        try {
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()){
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <columnCount ; i++) {
                String terminalID = rs.getString(i);
                list.add(terminalID);
                }
            }
        }catch (SQLException e){
            try {
                connection.rollback();
            }catch (SQLException e1){
                e1.printStackTrace();
            }
        }finally {
                JdbcPoolUtils.close(rs,pstmt,connection);
        }
        return list;

    }

    /**
     * 获取曹操专车的车辆的termid(写死了的)
     */
    public  List<String> getterminalID() throws SQLException {
        String sql = "SELECT ve.terminal_id from tb_vehicle ve LEFT JOIN vehicle_group gp ON ve.vehicle_group=gp.`\uFEFFID` WHERE gp.SuperiorID='100118'";
        List<String> terminalIds = run(sql);
        return terminalIds;
    }

    /**
     * 获取曹操专车的车辆配置信息(写死了的)
     */
    public  List<String> getconfiglist() throws SQLException {
        String sql = "SELECT * from adassetting WHERE GroupID='100118'";
        List<String> terminalIds = run(sql);
        return terminalIds;
    }
    /**
     * 获取曹操专车的某个termid的所有数据
     */
    public List<String> getTerminalData(String terminalid) throws SQLException {
        String sql =  "SELECT * from tb_vehicle WHERE terminal_id=%s ";
        sql= String.format(sql,terminalid);
        List<String> list = run(sql);
        return list;
    }
}
