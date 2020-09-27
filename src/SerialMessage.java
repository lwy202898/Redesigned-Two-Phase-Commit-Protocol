import java.io.*;

public class SerialMessage implements Serializable {

    public int msgType;
    public int commitId;
    public String filename;
    public byte[] img;
    public String[] sources;
    public String addr;

    public static final String ADDR_ALL = "all";

    public static final int USERNODE_YES = 1;
    public static final int USERNODE_ABORT = 2;
    public static final int USERNODE_ACK = 3;

    public static final int PREPARE = 4;
    public static final int COMMIT = 5;
    public static final int ABORT = 6;
    public static final int DONE = 7;

    public static String sourcesToString(String[] filenames) {
        if (filenames == null) {
            return "<null>";
        }
        StringBuffer sb = new StringBuffer();
        for (String source : filenames) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(source);
        }
        return sb.toString();
    }

    public SerialMessage(int _commitId, int _type, byte[] _img, String[] _sources, String _addr, String _filename) {
        commitId = _commitId;
        msgType = _type;
        img = _img;
        sources = _sources;
        addr = _addr;
        filename = _filename;
    }

    public SerialMessage(SerialMessage parent, int _type, String _addr) {
        commitId = parent.commitId;
        img = parent.img;
        sources = parent.sources;
        filename = parent.filename;

        addr = _addr;
        msgType = _type;
    }

    public SerialMessage() {
    }

    public String toString() {
        return "commitId: " + commitId
            + ", addr: " + (addr == null ? "null" : addr)
            + ", msgType: " + msgTypeToString(msgType)
            + ", sources: " + sourcesToString(sources);
    }

    private String msgTypeToString(int msgType) {
        switch (msgType) {
            case 1:
                return "USERNODE_YES";
            case 2:
                return "USERNODE_ABORT";
            case 3:
                return "USERNODE_ACK";
            case 4:
                return "PREPARE";
            case 5:
                return "COMMIT";
            case 6:
                return "ABORT";
            case 7:
                return "DONE";
            default:
                return "Unknown";
        }
    }

    public byte[] serialize() {
        try {
            ByteArrayOutputStream in = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(in);
            out.writeObject(this);
            return in.toByteArray();
        } catch(IOException e) {
            return new byte[]{};
        }
    }
    public static SerialMessage deserialize(byte[] rawdata) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(rawdata);
            ObjectInputStream result = new ObjectInputStream(in);
            return (SerialMessage) result.readObject();
        } catch(IOException e) {
            //Logger.log("MSG", "Got IOException when deserializing: " + e.toString());
            //e.printStackTrace(System.err);
            return null;
        } catch(ClassNotFoundException e) {
            e.printStackTrace(System.err);
            return null;
        }
    }
}
