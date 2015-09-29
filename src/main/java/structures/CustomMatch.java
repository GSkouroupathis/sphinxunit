package structures;

/**
 * Created by georskou on 28/09/15.
 */
public class CustomMatch {
    public int id;
    public CustomMsg customMsg;
    public int matchId;

    public CustomMatch(int id, CustomMsg customMsg, Integer matchId) {
        this.id = id;
        this.customMsg = customMsg;
        this.matchId = matchId;
    }

    @Override
    public String toString() {
        return customMsg.lineId + "-" + matchId + " " + customMsg.msg;
    }
}
