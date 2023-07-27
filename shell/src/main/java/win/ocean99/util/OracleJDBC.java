package win.ocean99.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * JDBC连接Oracle
 * @author ouyangjun
 */
@Slf4j
public class OracleJDBC {

    public static String driver = "oracle.jdbc.driver.OracleDriver";
    public static String url = "jdbc:oracle:thin:@localhost:1521:XE";
    public static String user = "CQCONTROL";
    public static String password = "CQCONTROL";

    /**
     * 获取Oracle数据库Connection
     */
    public static Connection getConnection() {
        return getConnection(driver, url, user, password);
    }

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection conn = null;
        try {
            Class.forName(driver); // 加载数据库驱动
            conn = DriverManager.getConnection(url, user, password); // 获取数据库连接
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}