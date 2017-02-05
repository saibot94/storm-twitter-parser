package cristis.stormproj.twitter;

import cristis.stormproj.db.JdbcTwitterInsertBolt;
import cristis.stormproj.db.TwitterLocationAggregationBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by darkg on 15-Jan-17.
 */
public class TwitterTopology {
    private final static String accessToken = "820588143955021826-j9p5rYFcRAQLHOKX1frlDZCWLYXX5nM";
    private final static String accessTokenSecret = "knGZtZrx3A6v5gXNIIHIUHJhC5Rh6OhiYvmheedMUL200";
    private final static String consumerKey = "lJXCy5aQ8GcD3MxvyP2cbeoeO";
    private final static String consumerSecret = "SVRRAhOn0HFDJfben5lnR4BpfQkkL86Zu1hA8meKYYIyePX7UG";

    private static IRichSpout buildTwitterSpout() {
        return new TwitterSpout(
                consumerKey,
                consumerSecret,
                accessToken,
                accessTokenSecret,
                new String[]{}
        );
    }
    private static IRichSpout buildTwitterSpout(String[] keywords) {
        return new TwitterSpout(
                consumerKey,
                consumerSecret,
                accessToken,
                accessTokenSecret,
                keywords
        );
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout spout = buildTwitterSpout();
        builder.setSpout("tweetspout", spout, 1);
        builder.setBolt("parsebolt", new TwitterParseBolt(), 5)
                .shuffleGrouping("tweetspout");
        builder.setBolt("sqlBolt", new JdbcTwitterInsertBolt(), 2)
                .shuffleGrouping("parsebolt");
        builder.setBolt("placeAggregateBolt", new TwitterLocationAggregationBolt())
                .shuffleGrouping("parsebolt");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(3);
        config.setDebug(true);


        cluster.submitTopology("testtwitter", config, builder.createTopology());
        Utils.sleep(100000);
        cluster.killTopology("testtwitter");
        cluster.shutdown();
    }
}
