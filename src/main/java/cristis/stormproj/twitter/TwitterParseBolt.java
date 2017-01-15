package cristis.stormproj.twitter;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by darkg on 15-Jan-17.
 */
public class TwitterParseBolt extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status twitterStatus = (Status) input.getValue(0);
        Double latitude = null;
        Double longitude = null;
        if(twitterStatus.getGeoLocation() != null){
            longitude = twitterStatus.getGeoLocation().getLongitude();
            latitude = twitterStatus.getGeoLocation().getLatitude();
        }
        TweetObj tweetObj = new TweetObj(
                twitterStatus.getId(),
                twitterStatus.getCreatedAt(),
                twitterStatus.getText(),
                twitterStatus.getUser().getName(),
                latitude,
                longitude);

        collector.emit(new Values(tweetObj));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetId"));
    }
}
