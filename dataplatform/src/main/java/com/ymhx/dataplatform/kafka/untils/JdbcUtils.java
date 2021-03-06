package com.ymhx.dataplatform.kafka.untils;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.beans.Transient;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Component
public class JdbcUtils {

    public List<String>   run(String sql) throws SQLException {
        DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement pstmt = null;
        List<String> list = new ArrayList<>();
        ResultSet rs=null;
        try {
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()){
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <=columnCount ; i++) {
                    String columnTypeName = rs.getMetaData().getColumnTypeName(i);//表字段类型
                        if ("DATETIME".equals(columnTypeName)){
                            continue;
                        }
                        String terminalID = rs.getString(i);
                        list.add(terminalID);

                }
            }
        }catch (SQLException e){
           e.printStackTrace();
        }finally {
            connection.close();
            pstmt.close();
           rs.close();

        }
        return list;

    }

    /**
     *  需要时间 查询车辆
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<String>   runoftime(String sql) throws SQLException {
        DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement pstmt = null;
        List<String> list = new ArrayList<>();
        ResultSet rs=null;
        try {
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()){
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <=columnCount ; i++) {
                    String columnTypeName = rs.getMetaData().getColumnTypeName(i);//表字段类型
                    if ("DATETIME".equals(columnTypeName)){
                        String format = dateFmt.format(rs.getDate(i));
                        list.add(format);
                        continue;
                    }
                    String terminalID = rs.getString(i);
                    list.add(terminalID);

                }
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            connection.close();
            pstmt.close();
            rs.close();

        }
        return list;

    }


    public List<String> query(String sql,Integer id,String starttime,String endtime) throws SQLException {

        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs=null;
        List<String> list = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setInt(1,id);
            pstmt.setString(2,starttime);
            pstmt.setString(3,endtime);
            rs = pstmt.executeQuery();
            while (rs.next()){
                //获取id数据
                int columnCount = rs.getMetaData().getColumnCount();
                if (columnCount>0){
                    String risk = rs.getString(7);
                    list.add(risk);
                    String reportid = rs.getString(1);
                    list.add(reportid);
                }
            }

        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if (connection!=null){
            connection.close();
            }
            if (pstmt!=null){
            pstmt.close();
            }
            if (rs!=null) {
                rs.close();
            }
        }
        return  list;

    }

    @Transactional
    public  void   save(String sql, Integer terminalid,Integer vehicleid,String number_plate,Integer groupid, String making, String date,String recodrtime) throws SQLException {
        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setInt(1,terminalid);
            stmt.setInt(2,vehicleid);
            stmt.setString(3,number_plate);
            stmt.setInt(4,groupid);
            stmt.setString(5,making);
            stmt.setString(6,date);
            stmt.setString(7,recodrtime);

            int i = stmt.executeUpdate();
            //处理结果
            if(i>0){

                System.out.println("stmt更新数据成功");

            }else{

                System.out.println("stmt更新数据失败");
            }
            stmt.addBatch();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            connection.close();
            stmt.close();
        }
    }
    @Transactional
    public  void   update(String sql, Integer terminalid, String making, String date) throws SQLException {
        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setString(1,making);
            stmt.setString(2,date);
            stmt.setInt(3,terminalid);
            int i = stmt.executeUpdate();
            //处理结果
            if(i>0){

                System.out.println("stmt更新数据成功");

            }else{

                System.out.println("stmt更新数据失败");
            }
            stmt.addBatch();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
           connection.close();
           stmt.close();
        }
    }
    @Transactional
    public  void   save(String sql,  String counts,String vehicleid, String startdate,String endtime) throws SQLException {
        Connection connection = DBUtils.getDataSource().getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setString(1,counts);
            stmt.setString(2,vehicleid);
            stmt.setString(3,startdate);
            stmt.setString(4,endtime);

            int i = stmt.executeUpdate();
            //处理结果
            if(i>0){
                System.out.println("stmt插入数据成功");
            }else{
                System.out.println("stmt插入数据失败");
            }
            stmt.addBatch();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            connection.close();
            stmt.close();
        }
    }


    /**
     * 获取曹操专车的车辆的vehicleid(写死了的)
     */
    public  List<String> getterminalID() throws SQLException {
        String sql = "SELECT ve.vehicle_id from tb_vehicle ve LEFT JOIN vehicle_group gp ON ve.vehicle_group=gp.`\uFEFFID` ";
        List<String> terminalIds = run(sql);
        return terminalIds;
    }

    /**
     * 获取曹操专车的车辆配置信息(写死了的)
     */
    public  List<String> getconfiglist() throws SQLException {
        String sql = "SELECT * from adassetting";
        List<String> terminalIds = run(sql);
        return terminalIds;
    }
    /**
     * 获取曹操专车的某个termid的所有数据
     */
    public List<String> getTerminalData(String terminalid) throws SQLException {
        String sql =  "SELECT * from tb_vehicle WHERE vehicle_id=%s ";
        sql= String.format(sql,terminalid);
        List<String> list = run(sql);
        return list;
    }
}
