package com.ymhx.dataplatform.kafka.untils;

public class DBUtils {
    //创建数据库连接池基础数据源的对象
    static DBpool pool=new DBpool();

    //返回连接池对象
    public static DataSource getDataSource() {
        return pool;
    }

}
