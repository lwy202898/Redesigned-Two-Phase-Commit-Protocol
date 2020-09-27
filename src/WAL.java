import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Time;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class WAL {

  private String id;
  private String filename;
  private ObjectOutputStream oos = null;
  private FileOutputStream fout = null;

  private List<SerialMessage> records = new LinkedList<>();
  private HashMap<Integer, Vector<SerialMessage>> operations = new HashMap<>();

  void recordsToOperations() {
    for (SerialMessage record : records) {
      Vector<SerialMessage> v = operations.get(record.commitId);
      if (v == null) {
        v = new Vector<SerialMessage>();
        v.add(record);
        operations.put(record.commitId, v);
      } else {
        v.add(record);
      }
    }
  }

  void readRecords() throws IOException, ClassNotFoundException {
    FileInputStream streamIn = new FileInputStream(filename);
    ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
    SerialMessage record = null;
    try {
      do {
        record = (SerialMessage) objectinputstream.readObject();
        Logger.log(id, "Read record from WAL file: " + record);
        if (record != null) {
          records.add(record);
        }
      } while (record != null);
    } catch (java.io.EOFException e) {
      Logger.log(id, "Finished reading the WAL file.");
    }
    recordsToOperations();
    streamIn.close();
  }

  public HashMap<Integer, Vector<SerialMessage>> getOperations() {
    return operations;
  }

  /**
   * Create a WAL log for id or read it if it exists.
   * @param _id
   */
  public WAL(String _id) throws IOException, ClassNotFoundException {
    this.id = _id;
    filename = _id+".wal";
    File file = new File(filename);
    if (file.exists()) {
      Logger.log(id, "WAL " + filename + " exists, reading previous records.");
      readRecords();
    } else {
      Logger.log(id, "WAL " + filename + " does not exist. Creating new file.");
      file.createNewFile();
    }
    fout = new FileOutputStream(filename, true);
    oos = new ObjectOutputStream(fout);
  }

  public void add(SerialMessage record, ProjectLib PL) {
    Logger.log(id, record.commitId + "# Trying to add msg to WAL: " + record);
    try{
      oos.writeObject(record);
      oos.flush();
      fout.flush();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    PL.fsync();
    Logger.log(id, record.commitId + "# Successfully added msg to WAL: " + record.toString());
  }

  // debug usage only
  public void close() {
    try {
      oos.close();
      fout.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
