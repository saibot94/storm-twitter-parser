package cristis.stormproj.db;

import cristis.stormproj.constants.Constants;
import cristis.stormproj.twitter.TweetObj;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Created by darkg on 15-Jan-17.
 */
public class JdbcTwitterInsertBolt extends BaseRichBolt {

    private static final String TABLE_NAME = "tweets";
    private static final java.text.SimpleDateFormat sdf =
            new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private OutputCollector collector;
    private Connection connection;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.connection = DriverManager.getConnection(Constants.JDBCURL, Constants.USER, Constants.PASSWORD);
            connection.setAutoCommit(true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table if exists " + TABLE_NAME);
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                    + "(id BIGINT PRIMARY KEY,"
                    + "created_at datetime NOT NULL,"
                    + "text varchar(256) CHARACTER SET utf8 NOT NULL,"
                    + "username varchar(256) CHARACTER SET utf8 NOT NULL,"
                    + "latitude DECIMAL,"
                    + "longitude DECIMAL)"
            );
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {

        TweetObj tweet = (TweetObj) input.getValue(0);
        String insertStatement = "INSERT INTO " + TABLE_NAME +
                "(id, created_at, text, username, latitude, longitude) VALUES " +
                "(?,?,?,?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(insertStatement)) {
            stmt.setLong(1, tweet.getId());
            stmt.setString(2, sdf.format(tweet.getCreatedAt()));
            stmt.setString(3, tweet.getText());
            stmt.setString(4, tweet.getUsername());
            if (tweet.getLatitude() != null)
                stmt.setDouble(5, tweet.getLatitude());
            else
                stmt.setNull(5, Types.DECIMAL);
            if (tweet.getLongitude() != null)
                stmt.setDouble(6, tweet.getLongitude());
            else stmt.setNull(6, Types.DECIMAL);
            stmt.executeUpdate();
            collector.emit(new Values(tweet.getId()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetId"));
    }
}
