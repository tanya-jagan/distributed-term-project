package traversal.distributed;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Starts worker processes (or threads) that connect to the coordinator. */
public interface WorkerLauncher {

	void launch(int count, String host, int port) throws IOException;

	/** Waits for the workers to exit; kills what is left after a grace period. */
	void close();

	/** Independent JVM processes: what a real deployment looks like. */
	class Processes implements WorkerLauncher {
		private final List<Process> processes = new ArrayList<Process>();
		private final long[] delaysMillis;
		private String maxMemory = "1g";

		/** @param delaysMillis optional artificial per-task delay of worker i (test/experiment hook). */
		public Processes(long... delaysMillis) {
			this.delaysMillis = delaysMillis;
		}

		public Processes setMaxMemory(String maxMemory) {
			this.maxMemory = maxMemory;
			return this;
		}

		@Override
		public void launch(int count, String host, int port) throws IOException {
			String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
			for (int i = 0; i < count; ++i) {
				List<String> cmd = new ArrayList<String>();
				cmd.add(java);
				cmd.add("-Xmx" + maxMemory);
				String logConfig = System.getProperty("logback.configurationFile");
				if (logConfig != null) cmd.add("-Dlogback.configurationFile=" + logConfig);
				cmd.add("-cp");
				cmd.add(System.getProperty("java.class.path"));
				cmd.add(Worker.class.getName());
				cmd.add(host);
				cmd.add(Integer.toString(port));
				cmd.add("--name");
				cmd.add("worker-" + i);
				if (i < delaysMillis.length && delaysMillis[i] > 0) {
					cmd.add("--delay-ms");
					cmd.add(Long.toString(delaysMillis[i]));
				}
				processes.add(new ProcessBuilder(cmd).inheritIO().start());
			}
		}

		@Override
		public void close() {
			for (Process p : processes) {
				try {
					if (!p.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) p.destroyForcibly();
				} catch (InterruptedException e) {
					p.destroyForcibly();
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	/** Threads inside this JVM that still talk over real TCP sockets; much faster to start, used by tests. */
	class Threads implements WorkerLauncher {
		private final List<Thread> threads = new ArrayList<Thread>();
		private final long[] delaysMillis;

		public Threads(long... delaysMillis) {
			this.delaysMillis = delaysMillis;
		}

		@Override
		public void launch(int count, String host, int port) {
			for (int i = 0; i < count; ++i) {
				Worker w = new Worker(host, port).setName("worker-" + i);
				if (i < delaysMillis.length) w.setTaskDelayMillis(delaysMillis[i]);
				Thread t = new Thread(w, "worker-" + i);
				t.setDaemon(true);
				threads.add(t);
				t.start();
			}
		}

		@Override
		public void close() {
			for (Thread t : threads) {
				try {
					t.join(10000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
