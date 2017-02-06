package cristis.stormproj.db;

import cristis.stormproj.constants.Constants;
import org.junit.Before;
import org.junit.Test;


import java.sql.*;

import static org.junit.Assert.*;

/**
 * Created by darkg on 15-Jan-17.
 */

public class JdbcConnectionTest {

    @Before
    public void setUp() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
    }

    @Test
    public void tryConnectToJdbc() {
        try (Connection conn = DriverManager.getConnection(Constants.JDBCURL, Constants.USER, Constants.PASSWORD)) {
            Statement stmt  = conn.createStatement();
            ResultSet res = stmt.executeQuery("show databases");
            while(res.next()) {
                System.out.println(res.getString("database"));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            fail();
        }
    }
}
