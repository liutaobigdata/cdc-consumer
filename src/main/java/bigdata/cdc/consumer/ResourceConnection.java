package bigdata.cdc.consumer;


import java.sql.Connection;
import java.sql.DriverManager;


public class ResourceConnection {



    public static Connection createMysqlConn(String url, String user, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection con = DriverManager.getConnection(url, user, password);
            return con;
        } catch (Exception e) {
        }
        return null;
    }

    public static Connection createRplConn(PgConfig pgconfig) {
        try {

            Class.forName("org.postgresql.Driver").newInstance();

            Connection con = DriverManager.getConnection(pgconfig.getUrl(), pgconfig.getUser(), pgconfig.getPassword());
            return con;
        } catch (Exception e) {
            System.out.println("pg库连接异常" + e.getLocalizedMessage());
        }
        return null;

    }
}
