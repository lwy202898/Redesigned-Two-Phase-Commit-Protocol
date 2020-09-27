/* Skeleton code for UserNode */


import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	private WAL wal;
	private static ProjectLib PL;
	private Timer timer = new Timer();

	private  ConcurrentHashMap<String, Integer> locks = new ConcurrentHashMap<>();

	public UserNode( String id ) throws Exception {
		myId = id;
		wal = new WAL(id);
	}

	private void retryIfNeeded() throws Exception {
		HashMap<Integer, Vector<SerialMessage>> operations = wal.getOperations();
		for (Integer cid : operations.keySet()) {
			Vector<SerialMessage> ops = operations.get(cid);
			SerialMessage lastMessage = ops.lastElement();
			if (lastMessage.msgType == SerialMessage.DONE) {
				Logger.log(myId, "[RetryIfNeeded] CID " + cid + " is done.");
				continue;
			}
			if (lastMessage.msgType == SerialMessage.USERNODE_ACK) {
				Logger.log(myId, cid + "# [RetryIfNeeded] resend ACK");
				retryAck(lastMessage);
			} else if (lastMessage.msgType == SerialMessage.COMMIT) {
				Logger.log(myId, cid + "# [RetryIfNeeded] reprocess COMMIT");
				retryProcessCommitMessage(lastMessage);
			} else if (lastMessage.msgType == SerialMessage.ABORT) {
				Logger.log(myId, cid + "# [RetryIfNeeded] reprocess ABORT");
				retryProcessAbortMessage(lastMessage);
			} else if (lastMessage.msgType == SerialMessage.USERNODE_YES) {
				Logger.log(myId, cid + "# [RetryIfNeeded] resend YES");
				retryYes(lastMessage);
			} else if (lastMessage.msgType == SerialMessage.USERNODE_ABORT) {
				Logger.log(myId, cid + "# [RetryIfNeeded] reesend ABORT");
				retryAbort(lastMessage);
			}
		}
	}

	private ArrayList<String> getMyFiles(SerialMessage msg) {
		ArrayList<String> myfiles = new ArrayList<>();
		if (msg.sources == null) {
			return myfiles;
		}
		for (String s : msg.sources) {
			String[] temp = s.split(":");
			String addr = temp[0];
			if (!addr.equals(myId)) {
				continue;
			}
			myfiles.add(temp[1]);
		}
		return myfiles;
	}

	private void prepareAbort(SerialMessage msg) {
		SerialMessage out = new SerialMessage(msg, SerialMessage.USERNODE_ABORT, "Server");
		wal.add(out, PL);
		byte[] toSend = out.serialize();
		ProjectLib.Message msg_out = new ProjectLib.Message("Server", toSend);
		PL.sendMessage(msg_out);
	}

	private void prepareYes(SerialMessage msg) {
		SerialMessage out = new SerialMessage(msg, SerialMessage.USERNODE_YES, "Server");
		wal.add(out, PL);
		byte[] toSend = out.serialize();
		ProjectLib.Message msg_out = new ProjectLib.Message("Server", toSend);
		PL.sendMessage(msg_out);
	}

	private void ack(SerialMessage msg) {
		SerialMessage out = new SerialMessage(msg, SerialMessage.USERNODE_ACK, "Server");
		wal.add(out, PL);
		byte[] toSend = out.serialize();
		ProjectLib.Message msg_out = new ProjectLib.Message("Server", toSend);
		PL.sendMessage(msg_out);
	}

	private void retryYes(SerialMessage msg) throws Exception {
		Logger.log(myId, msg.commitId + "# Start retry yes.");
		ArrayList<String> myFiles = getMyFiles(msg);
		Collections.sort(myFiles);

		boolean gotAllLocks = true;
		ArrayList<String> myLocks = new ArrayList<>();
		for (String file : myFiles) {
			File f = new File(file);
			if (!f.exists()) {
				Logger.log(myId, msg.commitId + "# requested file " + file + " does not exist.");
				gotAllLocks = false;
				break;
			} else {
				synchronized (locks) {
					if (locks.containsKey(file)) {
						Logger.log(myId, msg.commitId + "# Can't get lock for " + file);
						gotAllLocks = false;
						break;
					} else {
						Logger.log(myId, msg.commitId + "# Got lock for " + file);
						locks.put(file, msg.commitId);
						myLocks.add(file);
					}
				}
			}
		}

		if (!gotAllLocks) {
			Logger.log(myId, msg.commitId + "# Can't get all locks, this should not happen in retry.");
			throw new Exception(msg.commitId + "# Can't get all locks, this should not happen in retry.");
		}
		prepareYes(msg);
		Logger.log(myId, msg.commitId + "# Finished retry yes message.");
	}

	private void retryAbort(SerialMessage msg) throws Exception {
		prepareAbort(msg);
		Logger.log(myId, msg.commitId + "# Finished retry abort message.");
	}

	private void retryAck(SerialMessage msg) {
		ack(msg);
		Logger.log(myId, msg.commitId + "# Finished retry ack message.");
	}

	private void  processPrepareMessage(SerialMessage msg) {
		//TODO: wal.write(msg.commitId, Transaction.START);
		Logger.log(myId, msg.commitId + "# Start process PREPARE message.");
		ArrayList<String> myFiles = getMyFiles(msg);
		Collections.sort(myFiles);

		boolean gotAllLocks = true;
		ArrayList<String> myLocks = new ArrayList<>();
		for (String file : myFiles) {
			File f = new File(file);
			if (!f.exists()) {
				Logger.log(myId, msg.commitId + "# requested file " + file + " does not exist.");
				gotAllLocks = false;
				break;
			} else {
					synchronized (locks) {
						if (locks.containsKey(file) && locks.get(file) != msg.commitId) {
							Logger.log(myId, msg.commitId + "# Can't get lock for " + file);
							gotAllLocks = false;
							break;
						} else {
							Logger.log(myId, msg.commitId + "# Got lock for " + file);
							locks.put(file, msg.commitId);
							myLocks.add(file);
						}
					}
			}
		}

		if (!gotAllLocks) {
			Logger.log(myId, msg.commitId + "# Can't get all locks, releasing current locks.");
			for (String file : myLocks) {
				locks.remove(file);
				Logger.log(myId, msg.commitId + "# Can't get all locks, released " + file);
			}
			prepareAbort(msg);
		} else {
			Logger.log(myId, msg.commitId + "# Got all locks. Checking user permission ...");
			byte[] img = msg.img;
			String[] sources = new String[myFiles.size()];
			myFiles.toArray(sources);
			boolean result = PL.askUser(img, sources);

			if (result) {
				Logger.log(myId, msg.commitId + "# Got all locks. User said yes, sending yes response ...");
				prepareYes(msg);
			} else {
				Logger.log(myId, msg.commitId + "# Got all locks. User said no, sending abort response ...");
				prepareAbort(msg);
			}
		}
		Logger.log(myId, msg.commitId + "# Finished process PREPARE message.");
	}

	private void processCommitMessage(SerialMessage msg) {
		wal.add(msg, PL);
		ArrayList<String> myfiles = getMyFiles(msg);
		Collections.sort(myfiles);
		// TODO: right way is to use a log to record this transaction so all files can be removed together
		for (String file : myfiles) {
			Integer cid = locks.get(file);
			if (cid.intValue() != msg.commitId) {
				Logger.log(myId, "Warning: lock is hold by " + cid + " but committed by " + msg.commitId);
			} else {
				Logger.log(myId, cid + "# Removing " + file);
				try {
					File f = new File(file);
					if (f.delete()) {
						Logger.log(myId, cid + "# Removed " + file);
					} else {
						Logger.log(myId, cid + "# Error: failed to remove " + file);
					}
				} catch (Exception e) {
					Logger.log(myId, cid + "# Error: failed to remove " + file);
					e.printStackTrace(System.err);
				}
			}
		}
		ack(msg);
	}

	private void retryProcessCommitMessage(SerialMessage msg) throws Exception {
		Logger.log(myId, msg.commitId + "# [processCommitMessage] retry started.");
		ArrayList<String> myfiles = getMyFiles(msg);
		Collections.sort(myfiles);
		// TODO: right way is to use a log to record this transaction so all files can be removed together
		for (String file : myfiles) {
			File f = new File(file);
			f.delete();
			if (f.exists()) {
				throw new Exception("Deleting file file " + file + " failed.");
			}
		}
		SerialMessage newMsg = new SerialMessage(msg, SerialMessage.USERNODE_ACK, "Server");
		ack(newMsg);
	}

	private void processAbortMessage(SerialMessage msg) {
		wal.add(msg, PL);
		Logger.log(myId, msg.commitId + "# [processAbortMessage] started.");
		ArrayList<String> myfiles = getMyFiles(msg);
		// release locks in reverse order
		Collections.sort(myfiles, Collections.<String>reverseOrder());
		for (String file : myfiles) {
			Logger.log(myId, msg.commitId + "# Trying to get lock for " + file);
			Integer cid = locks.get(file);
			if (cid == null) {
				Logger.log(myId, msg.commitId + "# No locks hold for file " + file);
			} else {
				if (cid.intValue() != msg.commitId) {
					Logger.log(myId, msg.commitId + "# Warning: lock is hold by " + cid + " but aborted by " + msg.commitId);
				} else {
					locks.remove(file);
					Logger.log(myId, msg.commitId + "# [processAbortMessage] removed lock for " + file);
				}
			}
		}
		Logger.log(myId, msg.commitId + "# [processAbortMessage] Sending out abort ack");
		SerialMessage newMsg = new SerialMessage(msg, SerialMessage.USERNODE_ACK, "Server");
		ack(msg);
	}

	private void processDoneMessage(SerialMessage msg) {
		wal.add(msg, PL);
		Logger.log(myId, msg.commitId + "# [processDoneMessage] done.");
	}

	private void retryProcessAbortMessage(SerialMessage msg) {
		SerialMessage newMsg = new SerialMessage(msg, SerialMessage.USERNODE_ACK, "Server");
		ack(newMsg);
		Logger.log(myId, msg.commitId + "# [processAbortMessage] Resend the ack message.");
	}


	public boolean deliverMessage( ProjectLib.Message message ) {
		byte[] input = message.body;
		SerialMessage msg = SerialMessage.deserialize(input);
		msg.addr = message.addr;
		Logger.log(myId, msg.commitId + "# Got message from " + message.addr + ":" + msg.toString());

		switch (msg.msgType) {
			case SerialMessage.PREPARE:
				processPrepareMessage(msg);
				break;
			case SerialMessage.COMMIT:
				processCommitMessage(msg);
				break;
			case SerialMessage.ABORT:
				processAbortMessage(msg);
				break;
			case SerialMessage.DONE:
				processDoneMessage(msg);
				break;
			default:
				Logger.log(myId, "Got unknown type of message: " + msg);
		}
		return true;
	}

	public static void main (String[] args) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode un = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], un);
		un.retryIfNeeded();
	}
}
