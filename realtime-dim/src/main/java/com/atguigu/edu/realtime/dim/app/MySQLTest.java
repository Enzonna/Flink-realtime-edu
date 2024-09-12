package com.atguigu.edu.realtime.dim.app;

import java.sql.*;

public class MySQLTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        // 1. 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        // 2. 获取连接对象
        String url = "jdbc:mysql://hadoop102:3306/edu_config";
        String user = "root";
        String passwd = "000000";
        Connection connection = DriverManager.getConnection(url, user, passwd);

        // 3. 获取数据库操作对象（预编译）
        PreparedStatement preparedStatement = connection.prepareStatement("show databases");

        // 4. 执行，获取结果集
        ResultSet resultSet = preparedStatement.executeQuery();

        // 5. 解析结果集
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }

        // 6. 释放资源
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}