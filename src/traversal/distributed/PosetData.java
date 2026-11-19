package traversal.distributed;

import poset.Builder;
import poset.DefaultPoset;
import poset.VectorClock;

/**
 * The part of a poset that workers need to run bounded Lex: for each process,
 * the vector clocks of its real events (the dummy initial event is implicit).
 * Workers rebuild a {@link DefaultPoset} from it with the existing builder, so no
 * shared filesystem is required.
 */
public final class PosetData {
	public final int width;
	/** chains[pid] holds the clocks of events 1..k of the process, flattened (k * width ints). */
	public final int[][] chains;

	public PosetData(int width, int[][] chains) {
		this.width = width;
		this.chains = chains;
	}

	public static PosetData of(DefaultPoset poset) {
		int n = poset.width();
		int[][] chains = new int[n][];
		for (int pid = 0; pid < n; ++pid) {
			int events = poset.sizeOfChain(pid) - 1;
			chains[pid] = new int[events * n];
			for (int k = 0; k < events; ++k)
				for (int i = 0; i < n; ++i)
					chains[pid][k * n + i] = poset.getClock(pid, k + 1, i);
		}
		return new PosetData(n, chains);
	}

	public DefaultPoset build() {
		DefaultPoset poset = new DefaultPoset();
		Builder builder = poset.getBuilder();
		builder.beginBuild(width);
		for (int pid = 0; pid < width; ++pid) {
			int events = chains[pid].length / width;
			for (int k = 0; k < events; ++k) {
				VectorClock vc = new VectorClock(width);
				for (int i = 0; i < width; ++i) vc.set(i, chains[pid][k * width + i]);
				builder.addVectorClock(pid, vc);
			}
		}
		builder.endBuild();
		return poset;
	}
}
