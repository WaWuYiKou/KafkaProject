package com.ljl.offsetcustomstorage;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class DBUtils {
    private static String driver;
    private static String url;
    private static String user;
    private static String password;

    static {
        // 借助静态代码块保证配置文件只加载一次
        Properties prop = new Properties();
        try {
            // 加载配置文件，调用load()方法
            prop.load(DBUtils.class.getClassLoader().getResourceAsStream("db.properties"));
            // 从配置文件中获取数据为成员变量赋值
            driver = prop.getProperty("db.driver").trim();
            url = prop.getProperty("db.url").trim();
            user = prop.getProperty("db.user").trim();
            password = prop.getProperty("db.password").trim();
            // 加载驱动
            Class.forName(driver);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 动态绑定参数
     * @param pstmt
     * @param params
     */
    public static void bindParam(PreparedStatement pstmt, Object... params) {
        for (int i = 0; i < params.length; i++) {
            try {
                pstmt.setObject(i + 1, params[i]);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 预处理发生器
     * @param conn
     * @param sql
     * @return PreparedStatement
     */
    public static PreparedStatement getPstmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    /**
     * 获得连接方法
     * @return Connection
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取offset
     * @param sql
     * @param params
     * @return offset
     */
    public static long queryOffset(String sql, Object... params) {
        long offset = 0;
        Connection conn = getConn();
        PreparedStatement pstmt = getPstmt(conn, sql);
        bindParam(pstmt, params);
        ResultSet resultSet = null;
        try {
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong("topic_partition_offset");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet, pstmt, conn);
        }
        return offset;
    }

    /**
     * 更新偏移量
     * @param sql
     * @param offset
     */
    public static void update(String sql, Offset offset) {
        Connection conn = getConn();
        PreparedStatement pstmt = getPstmt(conn, sql);
        bindParam(
                pstmt,
                offset.getConsumer_group(),
                offset.getTopic(),
                offset.getTopic_partition_id(),
                offset.getTopic_partition_offset(),
                offset.getTimestamp()
        );
        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(null, pstmt, conn);
        }
    }

    public static void close(ResultSet resultSet, Statement stmt, Connection conn) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
