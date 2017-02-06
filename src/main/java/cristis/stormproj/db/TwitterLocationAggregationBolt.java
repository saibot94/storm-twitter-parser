package cristis.stormproj.db;

import cristis.stormproj.twitter.TweetObj;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.*;
import java.util.Map;

/**
 * Created by darkg on 2/5/2017.
 */
public class TwitterLocationAggregationBolt extends BaseRichBolt {
    private static final String jdbcUrl = "jdbc:mysql://localhost:3306/tweetdb?useUnicode=true&characterEncoding=utf8";
    private static final String TABLE_NAME = "tweetplaces";
    private static final String user = "root";

    private OutputCollector collector;
    private Connection connection;


    private String insertStatement = "INSERT INTO " + TABLE_NAME +
            "(country, fullname) VALUES " +
            "(?,?)";

    private String updateStatement = "UPDATE " + TABLE_NAME +
            "SET count = count + 1 WHERE country = ? and fullname = ?";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, user, null);
            connection.setAutoCommit(true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table if exists " + TABLE_NAME);
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                    + "(id BIGINT PRIMARY KEY AUTO_INCREMENT,"
                    + "country varchar(256) CHARACTER SET utf8 NOT NULL,"
                    + "fullname varchar(256) CHARACTER SET utf8 NOT NULL,"
                    + "count BIGINT NOT NULL DEFAULT 1)"
            );
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        TweetObj tweet = (TweetObj) input.getValue(0);
        if (tweet.getCountry() != null && tweet.getPlaceFullName() != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("select * from " + TABLE_NAME + " WHERE fullname = '" + tweet.getPlaceFullName() +
                        "' AND country = '" + tweet.getCountry() + "'");
                ResultSet rs = stmt.getResultSet();
                int rowcount = 0;
                if (rs.last()) {
                    rowcount = rs.getRow();
                    rs.beforeFirst(); // not rs.first() because the rs.next() below will move on, missing the first element
                }
                // insert
                try (PreparedStatement statement = rowcount == 0 ? connection.prepareStatement(insertStatement) : connection.prepareStatement(updateStatement)) {
                    statement.setString(1, tweet.getCountry());
                    statement.setString(2, tweet.getPlaceFullName());
                    statement.executeUpdate();
                    statement.close();
                    collector.emit(new Values(tweet.getId()));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetid"));
    }
}
