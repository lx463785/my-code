package com.ymhx.dataplatform.kafka.untils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcPoolUtils{
    private static JdbcPool pool = new JdbcPool();
        /**
         *  * 获取资源
         */
        public static Connection getConnection() throws SQLException {
            return pool.getConnection();
        }
        /**
         *  * 关闭资源
         *  * @param resultSet 查询返回的结果集，没有为空
         *  * @param statement   
         *  * @param connection
         */
        public static void close(ResultSet resultSet, Statement statement,
                                 Connection connection) {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                resultSet = null;
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

}
