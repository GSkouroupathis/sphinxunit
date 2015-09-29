package structures;

/**
 * Created by georskou on 28/09/15.
 */
public class CustomMsg {
    public int id;
    public int lineId;
    public int charId;
    public String msg;

    public CustomMsg (int id, int lineId, int charId, String msg) {
        this.id = id;
        this.lineId =  lineId;
        this.charId = charId;
        this.msg = msg;
    }
}
