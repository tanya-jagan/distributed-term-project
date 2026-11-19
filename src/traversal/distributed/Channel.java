package traversal.distributed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Framed binary protocol between coordinator and workers over one TCP
 * connection. Every message is a type byte followed by a fixed payload:
 *
 * <pre>
 * worker -> coordinator              coordinator -> worker
 * HELLO  name                        INIT  workerId mode chunkCuts validate poset
 * READY                              START
 * REQUEST                            ASSIGN taskId lower[w] upper[w]
 * CHUNK  taskId count cuts[count*w]  DONE
 * TASK_END taskId stats...
 * BYE    finalWaitNanos
 * ABORT  message                     ABORT message
 * </pre>
 *
 * Each connection has one reader and one writer thread at a time.
 */
public final class Channel implements Closeable {
	public static final byte HELLO = 1, INIT = 2, READY = 3, START = 4, REQUEST = 5, ASSIGN = 6, DONE = 7,
			CHUNK = 8, TASK_END = 9, BYE = 10, ABORT = 11;

	public static final int MODE_FULL = 0, MODE_SUMMARY = 1, MODE_COUNT = 2;

	/** Contents of INIT. */
	public static final class Init {
		public int workerId, mode, chunkCuts;
		public boolean validate;
		public PosetData poset;
	}

	/** Contents of TASK_END. */
	public static final class TaskEnd {
		public int taskId;
		public TaskSummary summary;
		public long computeNanos, sendNanos, waitNanos;
	}

	private final Socket socket;
	private final DataInputStream in;
	private final DataOutputStream out;
	private final Counter inCounter, outCounter;

	private static final class Counter {
		volatile long bytes;
	}

	public Channel(Socket socket) throws IOException {
		this.socket = socket;
		socket.setTcpNoDelay(true);
		inCounter = new Counter();
		outCounter = new Counter();
		in = new DataInputStream(new BufferedInputStream(new CountingIn(socket.getInputStream(), inCounter), 1 << 16));
		out = new DataOutputStream(new BufferedOutputStream(new CountingOut(socket.getOutputStream(), outCounter), 1 << 16));
	}

	public long bytesReceived() { return inCounter.bytes; }

	public long bytesSent() { return outCounter.bytes; }

	@Override
	public void close() {
		try {
			socket.close();
		} catch (IOException ignored) { }
	}

	public byte readType() throws IOException {
		return in.readByte();
	}

	private void begin(byte type) throws IOException {
		out.writeByte(type);
	}

	// ---- HELLO / READY / START / REQUEST / DONE / BYE / ABORT ----

	public void sendHello(String name) throws IOException {
		begin(HELLO);
		out.writeUTF(name);
		out.flush();
	}

	public String readHello() throws IOException {
		return in.readUTF();
	}

	public void sendSignal(byte type) throws IOException {
		begin(type);
		out.flush();
	}

	public void sendBye(long finalWaitNanos) throws IOException {
		begin(BYE);
		out.writeLong(finalWaitNanos);
		out.flush();
	}

	public long readBye() throws IOException {
		return in.readLong();
	}

	public void sendAbort(String message) {
		try {
			begin(ABORT);
			out.writeUTF(message.length() > 60000 ? message.substring(0, 60000) : message);
			out.flush();
		} catch (IOException ignored) { }
	}

	public String readAbort() throws IOException {
		return in.readUTF();
	}

	// ---- INIT ----

	public void sendInit(Init init) throws IOException {
		begin(INIT);
		out.writeInt(init.workerId);
		out.writeInt(init.mode);
		out.writeInt(init.chunkCuts);
		out.writeBoolean(init.validate);
		out.writeInt(init.poset.width);
		for (int pid = 0; pid < init.poset.width; ++pid) {
			out.writeInt(init.poset.chains[pid].length);
			writeInts(init.poset.chains[pid], init.poset.chains[pid].length);
		}
		out.flush();
	}

	public Init readInit() throws IOException {
		Init init = new Init();
		init.workerId = in.readInt();
		init.mode = in.readInt();
		init.chunkCuts = in.readInt();
		init.validate = in.readBoolean();
		int width = in.readInt();
		int[][] chains = new int[width][];
		for (int pid = 0; pid < width; ++pid) chains[pid] = readInts(in.readInt());
		init.poset = new PosetData(width, chains);
		return init;
	}

	// ---- ASSIGN ----

	public void sendAssign(Task task) throws IOException {
		begin(ASSIGN);
		out.writeInt(task.id);
		writeInts(task.lower, task.lower.length);
		writeInts(task.upper, task.upper.length);
		out.flush();
	}

	public Task readAssign(int width) throws IOException {
		int id = in.readInt();
		return new Task(id, readInts(width), readInts(width));
	}

	// ---- CHUNK ----

	public void sendChunk(int taskId, int[] cuts, int count, int width) throws IOException {
		begin(CHUNK);
		out.writeInt(taskId);
		out.writeInt(count);
		writeInts(cuts, count * width);
		out.flush();
	}

	/** @return {taskId, count}; the cuts follow and are read with {@link #readInts}. */
	public int[] readChunkHeader() throws IOException {
		return new int[] { in.readInt(), in.readInt() };
	}

	// ---- TASK_END ----

	public void sendTaskEnd(TaskEnd e, int width) throws IOException {
		begin(TASK_END);
		out.writeInt(e.taskId);
		TaskSummary s = e.summary;
		out.writeLong(s.count);
		out.writeLong(s.hash);
		out.writeLong(s.violations);
		out.writeLong(e.computeNanos);
		out.writeLong(e.sendNanos);
		out.writeLong(e.waitNanos);
		out.writeBoolean(s.first != null);
		if (s.first != null) {
			writeInts(s.first, width);
			writeInts(s.last, width);
		}
		out.flush();
	}

	public TaskEnd readTaskEnd(int width) throws IOException {
		TaskEnd e = new TaskEnd();
		e.taskId = in.readInt();
		long count = in.readLong(), hash = in.readLong(), violations = in.readLong();
		e.computeNanos = in.readLong();
		e.sendNanos = in.readLong();
		e.waitNanos = in.readLong();
		int[] first = null, last = null;
		if (in.readBoolean()) {
			first = readInts(width);
			last = readInts(width);
		}
		e.summary = new TaskSummary(count, hash, first, last, violations);
		return e;
	}

	// ---- bulk int arrays ----

	private void writeInts(int[] a, int length) throws IOException {
		byte[] bytes = new byte[length * 4];
		ByteBuffer.wrap(bytes).asIntBuffer().put(a, 0, length);
		out.write(bytes);
	}

	public int[] readInts(int length) throws IOException {
		byte[] bytes = new byte[length * 4];
		in.readFully(bytes);
		int[] a = new int[length];
		ByteBuffer.wrap(bytes).asIntBuffer().get(a);
		return a;
	}

	private static final class CountingIn extends FilterInputStream {
		private final Counter counter;

		CountingIn(InputStream in, Counter counter) {
			super(in);
			this.counter = counter;
		}

		@Override
		public int read() throws IOException {
			int b = super.read();
			if (b >= 0) ++counter.bytes;
			return b;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int n = super.read(b, off, len);
			if (n > 0) counter.bytes += n;
			return n;
		}
	}

	private static final class CountingOut extends FilterOutputStream {
		private final Counter counter;

		CountingOut(OutputStream out, Counter counter) {
			super(out);
			this.counter = counter;
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
			++counter.bytes;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
			counter.bytes += len;
		}
	}
}
