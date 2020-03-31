package com.ymhx.dataplatform.kafka.untils;



import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.LinkedList;



public class DBpool  implements DataSource {


  //数据库连接账号
    private String username="root";
    //数据库连接密码
    private String password="ymhx@2020";
    //驱动com.mysql.jdbc.Driver
    private String driver="com.mysql.jdbc.Driver";
    //数据库连接url  jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8","root","root
    private String url="jdbc:mysql://localhost:3306/bd_vehicle?characterEncoding=utf8&useSSL=true";
    //初始化数目
    private int init_count=3;
    //最大连接数
    private int max_count=10;
    //连接池里头的连接
    private int current_count=0;
    //连接池，存放链接
    private LinkedList<Connection> connections=new LinkedList<Connection>();


    //1、初始化连接放入连接池
    private void createBasicDataSource(){
        //初始化连接
        for(int i=0;i<init_count;i++){
            //记录当前连接数
            current_count++;
            //把链接放入连接池
            connections.addLast(createConnection());
        }
    }

    //2、建新连接的方法
    private Connection createConnection(){
        try {
            Class.forName(driver);
            //原始的目标对象
            final Connection conn= DriverManager.getConnection(url,username,password);
            /*******创建代理对象********/
            //创建代理对象
            Connection proxyConnection=(Connection)Proxy.newProxyInstance(
                    //类加载器
                    conn.getClass().getClassLoader(),
                    //目标接口对象
                    new Class[] {Connection.class},
                    //当调用conn对象的时候自动触发事务处理器
                    new InvocationHandler() {
                        @Override
                        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                            //方法返回值
                            Object result=null;
                            //当前执行的方法名
                            String methoName=method.getName();
                            //判断执行close()就把连接放入连接池
                            if ("close".equals(methoName)) {
                                //连接放入连接池
                                if(connections.size()<init_count){
                                    connections.addLast(conn);
                                }else{
                                    //如果连接池满了就关了连接
                                    try {
                                        current_count--;
                                        conn.close();
                                    } catch (SQLException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                            }else{
                                //调用目标方法对象
                                result=method.invoke(conn, args);
                            }

                            return result;
                        }
                    });
            return proxyConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //3、获取连接
    @Override
    public Connection getConnection() {
        //获取连接之前判断是否初始化
        if (connections.size()<=0) {
            createBasicDataSource();
        }
        //判断是否连接，有就去出，，就直拿
        if (connections.size()>0) {
            return connections.removeFirst();
        }
        //判断连接池有没有连接，如果没有达到最大连接，就创建
        if (current_count<max_count) {
            //记录当前连接使用数
            current_count++;
            //创建连接
            return createConnection();
        }
        //如果已达到最大连接数就，就抛出异常
        throw new RuntimeException("当前连接已达到最大连接数目！");
    }

    //4、释放链接
    public void releaseConnection(Connection conn) {
        //判断当前连接池数目如果少于初始化就放入池中
        if(connections.size()<init_count){
            connections.addLast(conn);
        }else{
            //如果连接池满了就关了连接
            try {
                current_count--;
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getInit_count() {
        return init_count;
    }

    public void setInit_count(int init_count) {
        this.init_count = init_count;
    }

    public int getMax_count() {
        return max_count;
    }

    public void setMax_count(int max_count) {
        this.max_count = max_count;
    }

    public int getCurrent_count() {
        return current_count;
    }

    public LinkedList<Connection> getConnections() {
        return connections;
    }


}
