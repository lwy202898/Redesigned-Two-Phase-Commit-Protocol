/* Skeleton code for Server */


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Server implements ProjectLib.CommitServing, ProjectLib.MessageHandling {
	private static ProjectLib PL;
	private int commitId = 0;
	private static final String SVR = "Server";
	private Timer timer = new Timer();

	private HashMap<String, CheckTask> checks = new HashMap<String, CheckTask>();

	private BlockingQueue<SerialMessage> reqQueue= new LinkedBlockingQueue<>();
	// the key is the commit Id
	private ConcurrentHashMap<Integer, RequestStatus> processMap = new ConcurrentHashMap<>();

	private WAL wal;

	class CheckTask extends TimerTask {
		SerialMessage msg;
		public final String myId;

		public CheckTask(String _myId, SerialMessage _msg) {
			this.myId = _myId;
			this.msg = _msg;
		}

		public void run() {
			Logger.log(myId,msg.commitId + "# Timer is up.");
			processTimeoutMessage(msg);
		}
	}

	private void sendMessageWithTimeout(SerialMessage msg) {
		byte[] toSend = msg.serialize();
		ProjectLib.Message msg_out = new ProjectLib.Message(msg.addr, toSend);
		PL.sendMessage(msg_out);
		addTimedCheck(msg);
	}

	private void processTimeoutMessage(SerialMessage msg) {
		if (msg.msgType == SerialMessage.COMMIT ||
				msg.msgType == SerialMessage.ABORT ||
				msg.msgType == SerialMessage.DONE) {
			Logger.log(SVR, msg.commitId + "# resending message " + msg.toString());
			sendMessageWithTimeout(msg);
		} else {
			RequestStatus rs = processMap.get(msg.commitId);
			String key = msg.addr;
			State state = rs.resultMap.get(key);
			abort(msg.commitId, rs);
			rs.state = State.SVR_STATE_WAITING_FOR_ABORT_ACK;
		}
	}

	private void addTimedCheck(SerialMessage msg) {
		String key = msg.addr + msg.commitId;
		CheckTask task = checks.get(key);
		if (task != null) {
			task.cancel();
			timer.purge();
		}
		task = new CheckTask(SVR, msg);
		checks.put(key, task);
		timer.schedule(task, 3 * 1000);
	}

	private static class RequestStatus{
		long startTimeMs;
		long lastProcessTimeMs;
		State state;
		// the addr field is not used
		SerialMessage msg;
		//the key is addr, the value is the response
		ConcurrentHashMap<String, State> resultMap = new ConcurrentHashMap<>();

		public RequestStatus(SerialMessage msg) {
			state = State.SVR_STATE_PREPARING;
			startTimeMs = System.currentTimeMillis();
			lastProcessTimeMs = startTimeMs;
			this.msg = msg;
		}

		public void processed() {
			this.lastProcessTimeMs = System.currentTimeMillis();
		}
	}

	private void retryIfNeeded() {
		HashMap<Integer, Vector<SerialMessage>> operations = wal.getOperations();
		for (Integer cid : operations.keySet()) {
			Vector<SerialMessage> ops = operations.get(cid);
			SerialMessage lastMessage = ops.lastElement();
			if (lastMessage.msgType == SerialMessage.DONE) {
				Logger.log(SVR, "CID " + cid + " is done.");
				continue;
			}
			int phase2 = 0;
			SerialMessage targetMsg = null;
			for (SerialMessage msg : ops) {
				if (msg.msgType == SerialMessage.COMMIT) {
					Logger.log(SVR, "Need to re-commit for cid " + cid);
					targetMsg = msg;
					phase2 = 1;
				} else if (msg.msgType == SerialMessage.ABORT) {
					Logger.log(SVR, "Need to re-abort for cid " + cid);
					targetMsg = msg;
					phase2 = 2;
				}
			}
			if (phase2 == 0) {
				targetMsg = ops.firstElement();
				Logger.log(SVR, "No aggrement reached before crash. Aborting  " + cid);
				retryAbort(targetMsg);
			} else if (phase2 == 1) {
				Logger.log(SVR, "Re-sending commit message for   " + cid);
				retryCommit(targetMsg);
			} else {
				Logger.log(SVR, "Re-sending abort message for   " + cid);
				retryAbort(targetMsg);
			}
		}
	}

	public Server() throws Exception{
		super();
		Logger.log(SVR, "Starting server ....");
		wal = new WAL(SVR);
	}

	public void startCommit( String filename, byte[] img, String[] sources) {
		int cid = commitId++;
		Logger.log(SVR, cid + "# Starting new commit request "
				+ " new file nane: " + filename
				+ " with sources: " + SerialMessage.sourcesToString(sources));

		SerialMessage msg = new SerialMessage(cid, SerialMessage.PREPARE, img, sources,
																					SerialMessage.ADDR_ALL, filename);
		RequestStatus requestStatus = new RequestStatus(msg);
		prepare(cid, requestStatus);
		processMap.put(cid, requestStatus);
	}

	private void retryPrepare(SerialMessage msg) {
		int cid = msg.commitId;
		Logger.log(SVR, cid + "# Retry prepare message ...");

		RequestStatus rs = new RequestStatus(msg);
		processMap.put(cid, rs);

		// the key is the addr, the value is that if we have sent it
		HashSet<String> sent = new HashSet<>();
		for (String source: rs.msg.sources) {
			String[] temp = source.split(":");
			String addr = temp[0];
			if (sent.contains(addr)) {
				continue;
			} else {
				sent.add(addr);
			}
			Logger.log(SVR, cid + "# Sending prepare request to user " + addr);
			SerialMessage out = new SerialMessage(rs.msg, SerialMessage.PREPARE, addr);
			sendMessageWithTimeout(out);
			rs.resultMap.put(addr, State.USER_STATE_CHECKING);
		}
	}

	private void prepare(Integer cid, RequestStatus rs) {
		SerialMessage logMsg = new SerialMessage(rs.msg, SerialMessage.PREPARE, SerialMessage.ADDR_ALL);
		wal.add(logMsg, PL);

		// the key is the addr, the value is that if we have sent it
		HashSet<String> sent = new HashSet<>();
		for (String source: rs.msg.sources) {
			String[] temp = source.split(":");
			String addr = temp[0];
			if (sent.contains(addr)) {
				continue;
			} else {
				sent.add(addr);
			}
			Logger.log(SVR, cid + "# Sending prepare request to user " + addr);
			SerialMessage msg = new SerialMessage(rs.msg, SerialMessage.PREPARE, addr);
			sendMessageWithTimeout(msg);
			rs.resultMap.put(addr, State.USER_STATE_CHECKING);
		}
	}

	private void retryCommit(SerialMessage msg) {
		Integer cid = msg.commitId;
		Logger.log(SVR, cid + "# Re-Sending out the commit msgs.");
		RequestStatus rs = new RequestStatus(msg);
		rs.state = State.SVR_STATE_WAITING_FOR_COMMIT_ACK;
		processMap.put(cid, rs);
		rs.resultMap = new ConcurrentHashMap<>();

		HashSet<String> sent = new HashSet<>();
		for (String src : msg.sources) {
			String[] tmp = src.split(":");
			String addr = tmp[0];
			rs.resultMap.put(addr, State.USER_STATE_COMMITING);
			if (sent.contains(addr)) {
				continue;
			} else {
				sent.add(addr);
			}
			Logger.log(SVR, cid + "# Re-sending out the commit msg to " + addr);
			SerialMessage retryCommit = new SerialMessage(rs.msg, SerialMessage.COMMIT, addr);
			sendMessageWithTimeout(retryCommit);
		}
		commitFile(cid, rs);
	}

	private void commit(int cid, RequestStatus rs) {
		Logger.log(SVR, cid + "# Sending out the commit msgs.");
		SerialMessage logMsg = new SerialMessage(rs.msg, SerialMessage.COMMIT, SerialMessage.ADDR_ALL);
		wal.add(logMsg, PL);

		// the key is the addr, the value is that if we have sent it
		HashSet<String> sent = new HashSet<>();
		for (String addr: rs.resultMap.keySet()) {
			if (sent.contains(addr)) {
				continue;
			} else {
				sent.add(addr);
			}
			SerialMessage msg = new SerialMessage(rs.msg, SerialMessage.COMMIT, addr);
			sendMessageWithTimeout(msg);
			rs.resultMap.put(addr, State.USER_STATE_COMMITING);
		}
		//Used for debugging the failure
		//wal.close();
		//throw new RuntimeException("Intentional error");
		commitFile(cid, rs);
	}

	private void commitFile(int cid, RequestStatus rs) {
		Logger.log(SVR, cid + "# Writing out the file: " + rs.msg.filename);
		try {
			File toWrite = new File(rs.msg.filename);
			toWrite.createNewFile();
			FileOutputStream writeTo = new FileOutputStream(toWrite);
			writeTo.write(rs.msg.img);
		} catch (IOException e) {
			// TODO: handle the write failure?
			System.err.println("An error occurred when writing to " + rs.msg.filename);
			e.printStackTrace();
		}
	}

	private void processYesMessage(SerialMessage msg) {
		RequestStatus rs = processMap.get(msg.commitId);
		if (rs == null) {
			Logger.log(SVR, msg.commitId + "#[processYes] Warning: no request status found for commitId ");
			return;
		}
		rs.processed();

		String srcAddr = msg.addr;
		State state = rs.resultMap.get(srcAddr);

		if (rs.state == State.SVR_STATE_PREPARING) {
			if (state != State.USER_STATE_CHECKING) {
				Logger.log(SVR, msg.commitId + "#[processYes] Warning: the status of commitId for addr " + srcAddr
						+ " is " + state.name() + ", not USER_STATE_CHECKING.");
				return;
			}
			rs.resultMap.put(srcAddr, State.USER_STATE_YES);
			boolean shouldCommit = true;
			for (String addr : rs.resultMap.keySet()) {
				State answer = rs.resultMap.get(addr);
				if (answer != State.USER_STATE_YES
						&& answer != State.USER_STATE_COMMITING
						&& answer != State.USER_STATE_COMMIT_ACKED) {
					Logger.log(SVR, msg.commitId + "# [processYes] Answer for " + addr + " is still " + answer.name());
					shouldCommit = false;
					break;
				}
			}

			if (shouldCommit) {
				commit(msg.commitId, rs);
				rs.state = State.SVR_STATE_WAITING_FOR_COMMIT_ACK;
			}
		} else if (rs.state == State.SVR_STATE_WAITING_FOR_ABORT_ACK) {
			Logger.log(SVR, msg.commitId + "# [processYes] Received yes answer for " + srcAddr
					+ " after aborted. The current state is " + state.name());
			if (state != State.USER_STATE_ABORTING && state != State.USER_STATE_ABORT_ACKED) {
				Logger.log(SVR, msg.commitId + "#[processYes] Warning: the status of commitId for addr " + srcAddr
						+ " is " + state.name() + ", not USER_STATE_ABORTING or USER_ABORT_ACKED.");
			}
			Logger.log(SVR, msg.commitId + "# Resending the abort message to " + srcAddr);
			SerialMessage resendMsg = new SerialMessage(rs.msg, SerialMessage.ABORT, srcAddr);
			sendMessageWithTimeout(resendMsg);
			rs.resultMap.put(srcAddr, State.USER_STATE_ABORTING);
		} else if (rs.state == State.SVR_STATE_WAITING_FOR_COMMIT_ACK) {
			Logger.log(SVR, msg.commitId + "# [processYes] Received yes answer for " + srcAddr
					+ " after committed. The current state is " + state.name());
			Logger.log(SVR, msg.commitId + "# Resending the commit message to " + srcAddr);
			SerialMessage resendMsg = new SerialMessage(rs.msg, SerialMessage.COMMIT, srcAddr);
			sendMessageWithTimeout(resendMsg);
			rs.resultMap.put(srcAddr, State.USER_STATE_COMMITING);
		} else {
			Logger.log(SVR,  msg.commitId + "# [processYes] Error: the status of commitId is "
					+ rs.state.name() + " not preparing or aborting.");
		}
	}

	private void finalize(RequestStatus rs) {
		Logger.log(SVR, rs.msg.commitId + "# is done.");
		SerialMessage msg = new SerialMessage(rs.msg, SerialMessage.DONE, SerialMessage.ADDR_ALL);
		wal.add(msg, PL);
		rs.state = State.SVR_STATE_DONE;
		sendDoneMsg(rs);
	}

	// TODO: we might want to separate process for ack for commit or abort
	private void processAckMessage(SerialMessage msg) {
		RequestStatus rs = processMap.get(msg.commitId);
		if (rs == null) {
			Logger.log(SVR, msg.commitId + "#[processAck] No request status found for commitId");
			return;
		}
		if (rs.state == State.SVR_STATE_WAITING_FOR_COMMIT_ACK) {
			processAckCommit(msg);
		} else if (rs.state == State.SVR_STATE_WAITING_FOR_ABORT_ACK ) {
			processAckAbort(msg);
		} else {
			Logger.log(SVR, msg.commitId + "#[processAck] Error: the status of commitId  is "
					+ rs.state.name() + ", not waiting for ack.");
		}
		rs.processed();
		return;
	}

	private void processAckCommit(SerialMessage msg) {
		RequestStatus rs = processMap.get(msg.commitId);
		String key = msg.addr;
		State state = rs.resultMap.get(key);
		if (state != State.USER_STATE_COMMITING) {
			Logger.log(SVR, msg.commitId + "# [processAck]Error: the status of commitId " + " for addr " + key
					+ " is " + state.name() + ", not USER_STATE_COMMITING.");
			return;
		}
		rs.resultMap.put(key, State.USER_STATE_COMMIT_ACKED);

		boolean isDone = true;
		for (String addr : rs.resultMap.keySet()) {
			State addrState = rs.resultMap.get(addr);
			if (addrState != State.USER_STATE_COMMIT_ACKED) {
				Logger.log(SVR, msg.commitId + "# [processAck]State for " + addr + " is still " + addrState.name());
				isDone = false;
				break;
			}
		}

		if (isDone) {
			finalize(rs);
		}
	}

	private void processAckAbort(SerialMessage msg) {
		RequestStatus rs = processMap.get(msg.commitId);
		String key = msg.addr;
		State state = rs.resultMap.get(key);
		if (state != State.USER_STATE_ABORTING) {
			Logger.log(SVR, msg.commitId + "# [processAck]Error: the status of commitId " + " for addr " + key
					+ " is " + state.name() + ", not USER_STATE_ABORTING.");
			return;
		}
		rs.resultMap.put(key, State.USER_STATE_ABORT_ACKED);

		boolean isDone = true;
		for (String addr : rs.resultMap.keySet()) {
			State addrState = rs.resultMap.get(addr);
			if (addrState != State.USER_STATE_ABORT_ACKED) {
				Logger.log(SVR, msg.commitId + "# [processAck]State for " + addr + " is still " + addrState.name());
				isDone = false;
				break;
			}
		}

		if (isDone) {
			finalize(rs);
		}
	}

	private void sendDoneMsg(RequestStatus rs) {
		int cid = rs.msg.commitId;
		Logger.log(SVR, cid + "# Sending out the done msg");
		for (String addr: rs.resultMap.keySet()) {
			SerialMessage msg = new SerialMessage(rs.msg, SerialMessage.DONE, addr);
			ProjectLib.Message userMsg = new ProjectLib.Message(addr, msg.serialize());
			PL.sendMessage(userMsg);
		}
	}

	private void abort(int cid, RequestStatus rs) {
		// TODO: remove the tmp file
		Logger.log(SVR, cid + "# Sending out the abort msg");
		for (String addr: rs.resultMap.keySet()) {
			SerialMessage msg = new SerialMessage(rs.msg, SerialMessage.ABORT, addr);
			sendMessageWithTimeout(msg);
			rs.resultMap.put(addr, State.USER_STATE_ABORTING);
		}
	}

	private void retryAbort(SerialMessage msg) {
		Integer cid = msg.commitId;
		RequestStatus rs = new RequestStatus(msg);
		rs.state = State.SVR_STATE_WAITING_FOR_ABORT_ACK;
		processMap.put(cid, rs);
		rs.resultMap = new ConcurrentHashMap<>();

		for (String src : msg.sources) {
			String[] tmp = src.split(":");
			String addr = tmp[0];
			rs.resultMap.put(addr, State.USER_STATE_ABORTING);
			Logger.log(SVR, cid + "# Re-sending out the abort msg to " + addr);
			SerialMessage retryAbort = new SerialMessage(rs.msg, SerialMessage.ABORT, addr);
			sendMessageWithTimeout(retryAbort);
		}
	}

	private void processAbortMessage(SerialMessage msg) {
		RequestStatus rs = processMap.get(msg.commitId);
		if (rs == null) {
			Logger.log(SVR,  msg.commitId + "# WARNING: no request status found for commitId ");
			return;
		}
		if (rs.state == State.SVR_STATE_DONE) {
			Logger.log(SVR, msg.commitId + "# WARNING: the status of commitId is " + rs.state.name());
			return;
		}
		rs.processed();

		String key = msg.addr;
		State state = rs.resultMap.get(key);
		Logger.log(SVR, msg.commitId + "# Addr " + key + " is aborting.");

		abort(msg.commitId, rs);
		// TBD: in some versions we need to wait for the aborting cnfirmation
		rs.state = State.SVR_STATE_WAITING_FOR_ABORT_ACK;
	}

	private void processMessage(SerialMessage msg) {
		switch(msg.msgType) {
			case SerialMessage.USERNODE_YES:
				processYesMessage(msg);
				break;
			case SerialMessage.USERNODE_ACK:
				processAckMessage(msg);
				break;
			case SerialMessage.USERNODE_ABORT:
				processAbortMessage(msg);
				break;
			default:
				Logger.log(SVR, "Got unknown type of message: " + msg);
		}
	}

	public void run() {
		while (true) {
			try {
				SerialMessage msg = reqQueue.poll(100, TimeUnit.MILLISECONDS);
				if (msg != null) {
					processMessage(msg);
				} else {
					//Logger.log(SVR, "IDLE: no message to process");
				}
				for (Integer cid : processMap.keySet()) {
						RequestStatus rs = processMap.get(cid);
						long currentMs = System.currentTimeMillis();
						// TODO : check the actual timeout
						if (currentMs - rs.lastProcessTimeMs > 3000) {
							//Logger.log(SVR, cid + "# have not been processed in 5 seconds. Mark it as time out.");
							//rs.state = SVR_STATE_DONE;
						}
				}
			} catch (InterruptedException e) {
				Logger.log(SVR, "Main loop got interrupted: " + e.getMessage());
			}
		}
	}

	@Override
	public boolean deliverMessage(ProjectLib.Message message) {
		try {
			SerialMessage msg = SerialMessage.deserialize(message.body);
			msg.addr = message.addr;
			String key = message.addr + msg.commitId;
			CheckTask task = checks.get(key);
			if (task != null) {
				task.cancel();
				timer.purge();
			}

			Logger.log(SVR, msg.commitId + "# Got message from " + message.addr + ": " + msg.toString());
			reqQueue.offer(msg);
			return true;
		} catch (Exception e) {
			Logger.log(SVR, "Receiver got exception: " + e);
			e.printStackTrace(System.err);
			return false;
		}
	}

	public static void main (String[] args) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv, srv);
		srv.retryIfNeeded();
		srv.run();
		PL.shutDown();
	}
}

