package traversal.distributed;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poset.DefaultPoset;
import poset.Poset;
import poset.VectorClock;
import traversal.enumerator.AbstractEnumerator;
import traversal.sequential.LexTraverser;

/**
 * An independent worker process. It connects to the coordinator over TCP,
 * rebuilds the poset it receives, then repeatedly asks for a task and enumerates
 * the task's interval [lower, upper] with the existing {@link LexTraverser}.
 * <p>
 * Usage: {@code java traversal.distributed.Worker <host> <port> [--name N] [--delay-ms MS]}
 * (--delay-ms sleeps before each task; it simulates a slow node for tests and experiments).
 */
public class Worker implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(Worker.class);

	private final String host;
	private final int port;
	private String name = ManagementFactory.getRuntimeMXBean().getName();
	private long taskDelayMillis = 0;

	public Worker(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public Worker setName(String name) {
		this.name = name;
		return this;
	}

	public Worker setTaskDelayMillis(long millis) {
		this.taskDelayMillis = millis;
		return this;
	}

	@Override
	public void run() {
		try {
			runChecked();
		} catch (Exception e) {
			logger.error("Worker {} failed: {}", name, e.toString());
		}
	}

	public void runChecked() throws IOException {
		Socket socket = new Socket(host, port);
		Channel ch = new Channel(socket);
		try {
			ch.sendHello(name);
			byte type = ch.readType();
			if (type == Channel.ABORT) throw new IOException("coordinator aborted: " + ch.readAbort());
			if (type != Channel.INIT) throw new IOException("expected INIT but got message type " + type);
			Channel.Init init = ch.readInit();
			DefaultPoset poset = init.poset.build();
			final int width = poset.width();
			LexTraverser lex = new LexTraverser();

			ch.sendSignal(Channel.READY);
			type = ch.readType();
			if (type != Channel.START) throw new IOException("expected START but got message type " + type);
			logger.debug("Worker {} (id {}) started.", name, init.workerId);

			ch.sendSignal(Channel.REQUEST);
			long waitStart = System.nanoTime();
			while (true) {
				type = ch.readType();
				long waitNanos = System.nanoTime() - waitStart;
				if (type == Channel.DONE) {
					ch.sendBye(waitNanos);
					break;
				} else if (type == Channel.ABORT) {
					throw new IOException("coordinator aborted: " + ch.readAbort());
				} else if (type != Channel.ASSIGN) {
					throw new IOException("unexpected message type " + type);
				}

				Task task = ch.readAssign(width);
				long taskStart = System.nanoTime();
				if (taskDelayMillis > 0) sleep(taskDelayMillis);
				TaskRecorder recorder = new TaskRecorder(ch, task.id, width, init.mode == Channel.MODE_FULL,
						init.mode != Channel.MODE_COUNT, init.chunkCuts, init.validate);
				lex.setEnumerator(recorder);
				try {
					enumerate(lex, poset, recorder, task);
				} catch (RuntimeException e) {
					ch.sendAbort("task " + task.id + " failed: " + e);
					throw e;
				}
				recorder.flush();

				Channel.TaskEnd end = new Channel.TaskEnd();
				end.taskId = task.id;
				end.summary = recorder.summary();
				end.sendNanos = recorder.sendNanos;
				end.computeNanos = System.nanoTime() - taskStart - recorder.sendNanos;
				end.waitNanos = waitNanos;
				ch.sendTaskEnd(end, width);
				waitStart = System.nanoTime();
			}
		} finally {
			ch.close();
		}
	}

	/**
	 * Same guards as the threaded traverser: a single-cut interval is one cut, an
	 * interval with lower not below upper is empty, otherwise bounded Lex.
	 */
	static void enumerate(LexTraverser lex, DefaultPoset poset, TaskRecorder recorder, Task task) {
		VectorClock l = Cuts.toVectorClock(task.lower), m = Cuts.toVectorClock(task.upper);
		if (l.equals(m)) recorder.enumerate(poset, l);
		else if (l.leq(m)) lex.boundedTraverse(poset, l, m);
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** Records the digest of one task's cuts and (in full mode) streams them to the coordinator in chunks. */
	static final class TaskRecorder extends AbstractEnumerator {
		private final Channel ch;
		private final int taskId, width, chunkCuts;
		private final boolean ship, digest, validate;
		private final int[] buffer;
		private int buffered = 0;
		private long count = 0, hash = 0, violations = 0;
		private int[] first, last;
		long sendNanos = 0;

		TaskRecorder(Channel ch, int taskId, int width, boolean ship, boolean digest, int chunkCuts, boolean validate) {
			this.ch = ch;
			this.taskId = taskId;
			this.width = width;
			this.ship = ship;
			this.digest = digest;
			this.chunkCuts = Math.max(1, chunkCuts);
			this.validate = validate;
			this.buffer = ship ? new int[this.chunkCuts * width] : null;
		}

		@Override
		protected void doEnumeration(Poset poset, VectorClock G) {
			++count;
			if (digest) {
				hash = Cuts.append(hash, Cuts.cutValue(G));
				if (last == null) {
					first = Cuts.toArray(G);
					last = first.clone();
				} else {
					if (!isGreater(G, last)) ++violations;
					for (int i = 0; i < width; ++i) last[i] = G.get(i);
				}
			}
			if (validate && !((DefaultPoset) poset).isConsistentCut(G)) ++violations;
			if (ship) {
				int off = buffered * width;
				for (int i = 0; i < width; ++i) buffer[off + i] = G.get(i);
				if (++buffered == chunkCuts) flush();
			}
		}

		private boolean isGreater(VectorClock g, int[] prev) {
			for (int i = 0; i < width; ++i) {
				if (g.get(i) != prev[i]) return g.get(i) > prev[i];
			}
			return false;
		}

		void flush() {
			if (buffered == 0) return;
			long t0 = System.nanoTime();
			try {
				ch.sendChunk(taskId, buffer, buffered, width);
			} catch (IOException e) {
				throw new IllegalStateException("cannot send results to the coordinator", e);
			}
			sendNanos += System.nanoTime() - t0;
			buffered = 0;
		}

		TaskSummary summary() {
			return new TaskSummary(count, hash, first, last, violations);
		}

		@Override
		protected void doClose() { }
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("usage: traversal.distributed.Worker <host> <port> [--name N] [--delay-ms MS]");
			System.exit(2);
		}
		Worker worker = new Worker(args[0], Integer.parseInt(args[1]));
		for (int i = 2; i + 1 < args.length; i += 2) {
			if (args[i].equals("--name")) worker.setName(args[i + 1]);
			else if (args[i].equals("--delay-ms")) worker.setTaskDelayMillis(Long.parseLong(args[i + 1]));
			else {
				System.err.println("unknown option " + args[i]);
				System.exit(2);
			}
		}
		try {
			worker.runChecked();
		} catch (Exception e) {
			logger.error("Worker failed: {}", e.toString());
			System.exit(1);
		}
	}
}
