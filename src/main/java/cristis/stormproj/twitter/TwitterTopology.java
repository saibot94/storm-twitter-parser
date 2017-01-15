package cristis.stormproj.twitter;

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

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout spout = buildTwitterSpout();
        builder.setSpout("tweetspout", spout, 2);
        builder.setBolt("printbolt", new TwitterParseBolt())
                .shuffleGrouping("tweetspout");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);


        cluster.submitTopology("testtwitter", config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("testtwitter");
        cluster.shutdown();
    }
}
