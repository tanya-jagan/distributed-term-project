package traversal.distributed.partition;

import java.util.ArrayList;
import java.util.List;

import poset.DefaultPoset;
import traversal.distributed.Task;

/**
 * Splits the lattice into lexicographically contiguous ranges, so the
 * concatenation of the tasks' outputs in task order is already globally sorted.
 * <p>
 * An interval [l,m] of consistent cuts is split on its first non-fixed process
 * d: one child per value v of G[d]. Because the consistent cuts are closed under
 * join and meet, every child is again an interval of the lattice:
 * <ul>
 * <li>lower = join(l, vc(d,v)), the least consistent cut at or above l that
 * contains event v of process d;</li>
 * <li>upper = greatest consistent cut below m with G[d] &lt;= v.</li>
 * </ul>
 * A child is non-empty exactly when lower &lt;= m.
 */
public class LexRangePartitioner implements Partitioner {

	@Override
	public String name() {
		return "lexrange";
	}

	@Override
	public List<Task> partition(DefaultPoset poset, int targetTasks) {
		final int n = poset.width();
		int[] l = new int[n];
		int[] m = new int[n];
		for (int i = 0; i < n; ++i) m[i] = poset.sizeOfChain(i) - 1;

		List<int[][]> ranges = new ArrayList<int[][]>();
		ranges.add(new int[][] { l, m });
		while (ranges.size() < targetTasks) {
			int best = -1;
			double bestScore = -1;
			for (int i = 0; i < ranges.size(); ++i) {
				if (splitDimension(ranges.get(i)) < 0) continue;
				double score = volume(ranges.get(i));
				if (score > bestScore) {
					bestScore = score;
					best = i;
				}
			}
			if (best < 0) break; // every range is a single cut
			int[][] range = ranges.remove(best);
			ranges.addAll(best, split(poset, range, splitDimension(range)));
		}

		List<Task> tasks = new ArrayList<Task>(ranges.size());
		for (int i = 0; i < ranges.size(); ++i)
			tasks.add(new Task(i, ranges.get(i)[0], ranges.get(i)[1]));
		return tasks;
	}

	/** First process whose clock is not yet fixed, or -1 for a single-cut range. */
	private static int splitDimension(int[][] range) {
		for (int d = 0; d < range[0].length; ++d)
			if (range[0][d] < range[1][d]) return d;
		return -1;
	}

	/** Log of the bounding-box size; a cheap proxy for the amount of work. */
	private static double volume(int[][] range) {
		double v = 0;
		for (int i = 0; i < range[0].length; ++i)
			v += Math.log(range[1][i] - range[0][i] + 1);
		return v;
	}

	static List<int[][]> split(DefaultPoset poset, int[][] range, int d) {
		final int n = poset.width();
		int[] l = range[0], m = range[1];
		List<int[][]> children = new ArrayList<int[][]>();
		for (int v = l[d]; v <= m[d]; ++v) {
			int[] lower = l.clone();
			for (int i = 0; i < n; ++i)
				lower[i] = Math.max(lower[i], poset.getClock(d, v, i));
			if (!leq(lower, m)) continue; // no consistent cut has G[d] == v
			int[] bound = m.clone();
			bound[d] = v;
			children.add(new int[][] { lower, greatestConsistentBelow(poset, bound) });
		}
		return children;
	}

	/**
	 * The greatest consistent cut that is &lt;= bound. Consistent cuts are closed
	 * under join, so it exists; it is reached by lowering any clock whose event
	 * depends on events beyond the current cut.
	 */
	static int[] greatestConsistentBelow(DefaultPoset poset, int[] bound) {
		final int n = poset.width();
		int[] g = bound.clone();
		boolean changed;
		do {
			changed = false;
			for (int j = 0; j < n; ++j) {
				while (g[j] > 0 && !dependenciesWithin(poset, g, j)) {
					--g[j];
					changed = true;
				}
			}
		} while (changed);
		return g;
	}

	private static boolean dependenciesWithin(DefaultPoset poset, int[] g, int j) {
		for (int i = 0; i < g.length; ++i)
			if (poset.getClock(j, g[j], i) > g[i]) return false;
		return true;
	}

	private static boolean leq(int[] a, int[] b) {
		for (int i = 0; i < a.length; ++i)
			if (a[i] > b[i]) return false;
		return true;
	}
}
