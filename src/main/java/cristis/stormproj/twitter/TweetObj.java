package cristis.stormproj.twitter;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by darkg on 15-Jan-17.
 */

@Data
@AllArgsConstructor
public class TweetObj implements Serializable {
    private long id;
    private Date createdAt;
    private String text;
    private String username;
    private String country = null;
    private String placeFullName = null;
    private Double latitude = null;
    private Double longitude = null;
}
