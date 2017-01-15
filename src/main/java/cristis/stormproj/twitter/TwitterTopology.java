package cristis.stormproj.twitter;

import cristis.stormproj.db.JdbcTwitterInsertBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by darkg on 15-Jan-17.
 */
public class TwitterTopology {
    private final static String accessToken = "";
    private final static String accessTokenSecret = "";
    private final static String consumerKey = "";
    private final static String consumerSecret = "";

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
        IRichSpout spout = buildTwitterSpout(new String[]{"spacex", "elon musk"});
        builder.setSpout("tweetspout", spout, 1);
        builder.setBolt("parsebolt", new TwitterParseBolt(), 5)
                .shuffleGrouping("tweetspout");
        builder.setBolt("sqlBolt", new JdbcTwitterInsertBolt(), 2)
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
