package traversal.distributed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import poset.DefaultPoset;
import poset.Poset;
import poset.VectorClock;
import traversal.Traverser;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.Partitioner;
import traversal.enumerator.NOPEnumerator;

/**
 * Coordinator of the distributed lexical enumeration.
 * <p>
 * It partitions the lattice into {@link Task}s, accepts connections from
 * independent {@link Worker} processes, hands out tasks (statically or
 * dynamically) and merges the workers' sorted result streams so that the
 * enumerator chain sees every consistent cut exactly once, in global
 * lexicographic order.
 * <p>
 * Existing traversers are untouched; this class only reuses {@link Traverser}'s
 * enumerator chaining, monitoring and logging.
 */
public class DistributedLexTraverser extends Traverser {

	public enum Schedule { STATIC, DYNAMIC }

	/**
	 * FULL: workers ship every cut and the coordinator emits them in order to
	 * the enumerators. SUMMARY: workers ship only count/hash/first/last per task
	 * (for inputs too large to transfer); the coordinator verifies the order from
	 * the ranges. COUNT: workers ship only the count (the cheapest, comparable
	 * to ConcurrentUnordered with a counter; no order verification). In SUMMARY
	 * and COUNT mode enumerators are not invoked.
	 */
	public enum OutputMode { FULL, SUMMARY, COUNT }

	private int workers = 2;
	private Schedule schedule = Schedule.DYNAMIC;
	private OutputMode outputMode = OutputMode.FULL;
	private Partitioner partitioner = new LexRangePartitioner();
	private int granularity = 4;
	private boolean validate = false;
	private int chunkCuts = 0;
	private String bindHost = "127.0.0.1";
	private int port = 0;
	private long timeoutMillis = 60000;
	private WorkerLauncher launcher = null;

	private RunStats stats;
	private SummaryVerifier summary;

	public DistributedLexTraverser setWorkers(int workers) {
		if (workers < 1) throw new IllegalArgumentException("need at least one worker");
		this.workers = workers;
		return this;
	}

	public DistributedLexTraverser setSchedule(Schedule schedule) {
		this.schedule = schedule;
		return this;
	}

	public DistributedLexTraverser setOutputMode(OutputMode mode) {
		this.outputMode = mode;
		return this;
	}

	public DistributedLexTraverser setPartitioner(Partitioner partitioner) {
		this.partitioner = partitioner;
		return this;
	}

	/** Tasks per worker requested from partitioners that can choose their granularity. */
	public DistributedLexTraverser setGranularity(int tasksPerWorker) {
		this.granularity = Math.max(1, tasksPerWorker);
		return this;
	}

	/** Workers verify that each cut is consistent and strictly increasing before reporting it. */
	public DistributedLexTraverser setValidate(boolean validate) {
		this.validate = validate;
		return this;
	}

	/** Cuts per result message; 0 picks a size that keeps messages around 256 KB. */
	public DistributedLexTraverser setChunkCuts(int chunkCuts) {
		this.chunkCuts = chunkCuts;
		return this;
	}

	public DistributedLexTraverser setBindHost(String host) {
		this.bindHost = host;
		return this;
	}

	/** Port to listen on; 0 picks a free port. */
	public DistributedLexTraverser setPort(int port) {
		this.port = port;
		return this;
	}

	/** Bounds waiting for workers to connect and become ready. */
	public DistributedLexTraverser setTimeoutMillis(long millis) {
		this.timeoutMillis = millis;
		return this;
	}

	/** Starts the workers itself; null (default) waits for externally started workers. */
	public DistributedLexTraverser setLauncher(WorkerLauncher launcher) {
		this.launcher = launcher;
		return this;
	}

	public RunStats getStats() {
		return stats;
	}

	/** In SUMMARY mode, the verified count/digest of the run. */
	public SummaryVerifier getSummary() {
		return summary;
	}

	@Override
	protected void doTraverse(Poset gPoset) {
		final DefaultPoset poset = (DefaultPoset) gPoset;
		final int width = poset.width();
		final long runStart = System.nanoTime();
		RunStats st = new RunStats();
		st.partitioner = partitioner.name();
		st.schedule = schedule.name();
		st.outputMode = outputMode.name();
		st.workers = workers;

		List<Task> tasks = partitioner.partition(poset, workers * granularity);
		st.partitionNanos = System.nanoTime() - runStart;
		st.tasks = tasks.size();
		for (int i = 0; i < tasks.size(); ++i)
			if (tasks.get(i).id != i) throw new IllegalStateException("task ids must equal list positions: " + tasks.get(i));
		logger.info("{} tasks from the {} partitioner.", tasks.size(), partitioner.name());

		final OrderedMerger merger;
		ResultCollector collector;
		if (outputMode == OutputMode.FULL) {
			final VectorClock g = new VectorClock(width);
			final traversal.enumerator.Enumerator sink = enumerator;
			merger = new OrderedMerger(width, tasks, new OrderedMerger.CutSink() {
				@Override
				public void accept(int[] data, int off) {
					for (int i = 0; i < width; ++i) g.set(i, data[off + i]);
					sink.enumerate(poset, g);
				}
			});
			collector = merger;
			summary = null;
		} else {
			merger = null;
			summary = new SummaryVerifier(tasks, outputMode == OutputMode.COUNT);
			collector = summary;
			if (!(enumerator instanceof NOPEnumerator))
				logger.warn("{} mode does not invoke enumerators; use getSummary() for the count and digest.", outputMode);
		}

		TaskScheduler scheduler = schedule == Schedule.STATIC ? TaskScheduler.statik(tasks, workers)
				: TaskScheduler.dynamic(tasks);
		PosetData data = PosetData.of(poset);
		int cutsPerChunk = chunkCuts > 0 ? chunkCuts : Math.max(16, Math.min(8192, 65536 / width));

		Run run = new Run(width, collector, scheduler, data, cutsPerChunk);
		try {
			run.execute(st);
		} finally {
			if (launcher != null) launcher.close();
		}

		if (merger != null) {
			merger.finish();
			st.cuts = merger.emittedCount();
			st.mergeNanos = merger.mergeNanos();
			st.peakBufferedCuts = merger.peakBufferedCuts();
		} else {
			summary.finish();
			st.cuts = summary.totalCount();
			if (!summary.isOrderVerified() && outputMode != OutputMode.COUNT)
				logger.info("Task ranges interleave (non-contiguous partitioner): only the count is verified, not the order.");
		}
		long sum = 0;
		for (RunStats.WorkerStats w : st.perWorker) sum += w.cuts;
		if (sum != st.cuts)
			throw new IllegalStateException("workers reported " + sum + " cuts but " + st.cuts + " were merged");
		st.setupNanos = run.setupNanos;
		st.traverseNanos = run.traverseNanos;
		stats = st;
		logger.info("{} cuts from {} workers in {} ms.", st.cuts, workers, String.format("%.1f", RunStats.ms(st.totalNanos())));
	}

	/** One execution: the server socket and one session per worker connection. */
	private class Run {
		final int width, cutsPerChunk;
		final ResultCollector collector;
		final TaskScheduler scheduler;
		final PosetData data;
		final List<Session> sessions = new java.util.concurrent.CopyOnWriteArrayList<Session>();
		final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
		final CountDownLatch ready = new CountDownLatch(workers);
		final AtomicLong startNanos = new AtomicLong();
		long setupNanos, traverseNanos;
		ServerSocket server;

		Run(int width, ResultCollector collector, TaskScheduler scheduler, PosetData data, int cutsPerChunk) {
			this.width = width;
			this.collector = collector;
			this.scheduler = scheduler;
			this.data = data;
			this.cutsPerChunk = cutsPerChunk;
		}

		void execute(RunStats st) {
			long t0 = System.nanoTime();
			try {
				server = new ServerSocket(port, Math.max(50, workers), InetAddress.getByName(bindHost));
				server.setSoTimeout((int) timeoutMillis);
				logger.info("Coordinator listening on {}:{}, waiting for {} workers.", bindHost, server.getLocalPort(), workers);
				if (launcher != null) launcher.launch(workers, bindHost, server.getLocalPort());
				for (int id = 0; id < workers; ++id) {
					Socket s = server.accept();
					Session session = new Session(id, new Channel(s));
					sessions.add(session);
					session.thread.start();
				}
			} catch (SocketTimeoutException e) {
				fail(new IllegalStateException("timed out waiting for " + workers + " workers to connect (" + sessions.size() + " did)", e));
			} catch (IOException e) {
				fail(e);
			}
			// the START barrier releases once every worker is ready; the clock for the traversal starts there
			for (Session s : sessions) {
				try {
					s.thread.join();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					fail(e);
				}
			}
			long end = System.nanoTime();
			closeServer();
			if (failure.get() != null)
				throw new IllegalStateException("distributed traversal failed: " + failure.get(), failure.get());
			long start = startNanos.get();
			setupNanos = start - t0 + st.partitionNanos;
			traverseNanos = end - start;
			for (Session s : sessions) {
				s.stats.tailIdleNanos = end - s.doneNanos;
				st.perWorker.add(s.stats);
			}
		}

		void fail(Throwable t) {
			if (failure.compareAndSet(null, t)) {
				for (Session s : sessions) {
					s.channel.sendAbort("coordinator failed: " + t);
					s.channel.close();
				}
				closeServer();
			}
		}

		void closeServer() {
			try {
				if (server != null) server.close();
			} catch (IOException ignored) { }
		}

		private class Session implements Runnable {
			final int id;
			final Channel channel;
			final Thread thread;
			final RunStats.WorkerStats stats = new RunStats.WorkerStats();
			long doneNanos;

			Session(int id, Channel channel) {
				this.id = id;
				this.channel = channel;
				this.stats.id = id;
				this.thread = new Thread(this, "session-" + id);
			}

			@Override
			public void run() {
				try {
					serve();
				} catch (Throwable t) {
					fail(t);
					ready.countDown(); // never leave the other sessions waiting at the barrier
				} finally {
					channel.close();
				}
			}

			private void serve() throws Exception {
				expect(Channel.HELLO);
				stats.name = channel.readHello();
				Channel.Init init = new Channel.Init();
				init.workerId = id;
				init.mode = outputMode == OutputMode.FULL ? Channel.MODE_FULL
						: outputMode == OutputMode.SUMMARY ? Channel.MODE_SUMMARY : Channel.MODE_COUNT;
				init.chunkCuts = cutsPerChunk;
				init.validate = validate;
				init.poset = data;
				channel.sendInit(init);
				expect(Channel.READY);
				ready.countDown();
				if (!ready.await(timeoutMillis, TimeUnit.MILLISECONDS))
					throw new IllegalStateException("workers did not become ready in time");
				if (failure.get() != null) return;
				startNanos.compareAndSet(0, System.nanoTime());
				channel.sendSignal(Channel.START);

				while (true) {
					byte type = channel.readType();
					switch (type) {
					case Channel.REQUEST:
						dispatch();
						break;
					case Channel.CHUNK: {
						int[] header = channel.readChunkHeader();
						int[] cuts = channel.readInts(header[1] * width);
						collector.chunk(header[0], cuts, header[1]);
						break;
					}
					case Channel.TASK_END: {
						Channel.TaskEnd end = channel.readTaskEnd(width);
						collector.taskFinished(end.taskId, end.summary);
						++stats.tasks;
						stats.cuts += end.summary.count;
						stats.computeNanos += end.computeNanos;
						stats.sendNanos += end.sendNanos;
						stats.waitNanos += end.waitNanos;
						dispatch();
						break;
					}
					case Channel.BYE:
						stats.waitNanos += channel.readBye();
						stats.bytesToWorker = channel.bytesSent();
						stats.bytesFromWorker = channel.bytesReceived();
						return;
					case Channel.ABORT:
						throw new IllegalStateException("worker " + stats.name + " aborted: " + channel.readAbort());
					default:
						throw new IllegalStateException("unexpected message type " + type + " from " + stats.name);
					}
				}
			}

			private void expect(byte expected) throws IOException {
				byte type = channel.readType();
				if (type == Channel.ABORT) throw new IllegalStateException("worker aborted: " + channel.readAbort());
				if (type != expected) throw new IllegalStateException("expected message " + expected + " but got " + type);
			}

			private void dispatch() throws IOException {
				long t0 = System.nanoTime();
				Task task = scheduler.next(id);
				if (task == null) {
					channel.sendSignal(Channel.DONE);
					doneNanos = System.nanoTime();
				} else {
					collector.taskStarted(task);
					channel.sendAssign(task);
				}
				stats.schedulingNanos += System.nanoTime() - t0;
			}
		}
	}
}
