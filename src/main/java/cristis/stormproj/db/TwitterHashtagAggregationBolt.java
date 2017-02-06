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
import java.util.Map;

/**
 * Created by darkg on 2/6/2017.
 */
public class TwitterHashtagAggregationBolt extends BaseRichBolt{
    private static final String TABLE_NAME = "tweethashtags";

    private String insertStatement = "INSERT INTO " + TABLE_NAME +
            "(text) VALUES " +
            "(?)";

    private String updateStatement = "UPDATE " + TABLE_NAME +
            "SET count = count + 1 WHERE text = ? ";
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
                    + "(id BIGINT PRIMARY KEY AUTO_INCREMENT,"
                    + "text varchar(256) CHARACTER SET utf8 NOT NULL,"
                    + "count BIGINT NOT NULL DEFAULT 1"
                    + ")"
            );
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        TweetObj tweet = (TweetObj) input.getValue(0);
        for (String hashtag : tweet.getHashtags()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("select * from " + TABLE_NAME + " WHERE text = '" + hashtag + "' ");
                ResultSet rs = stmt.getResultSet();
                int rowcount = 0;
                if (rs.last()) {
                    rowcount = rs.getRow();
                    rs.beforeFirst(); // not rs.first() because the rs.next() below will move on, missing the first element
                }
                // insert
                try (PreparedStatement statement = rowcount == 0 ? connection.prepareStatement(insertStatement) : connection.prepareStatement(updateStatement)) {
                    statement.setString(1, hashtag);
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
