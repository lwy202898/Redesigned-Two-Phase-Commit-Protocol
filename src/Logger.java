public class Logger {

  private static boolean DEBUG = true;

  public static void log(String obj, String msg) {
    if (DEBUG) {
      long time = System.currentTimeMillis();
      String s = obj;
      if (obj.equals("Server")) {
        s = "s";
      }
      System.err.println("[" + time + "]" + "<" + s + "> " + msg);
    }
  }

  private Logger() {
  }
}
