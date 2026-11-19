package traversal.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import poset.Builder;
import poset.DefaultPoset;
import poset.PosetParser;
import poset.VectorClock;
import traversal.enumerator.AbstractEnumerator;
import traversal.sequential.LexTraverser;
import poset.Poset;

/** Posets, oracles and collectors shared by the tests. */
public final class TestKit {
	private TestKit() { }

	/** Collects copies of all enumerated cuts, in enumeration order. */
	public static class CollectingEnumerator extends AbstractEnumerator {
		public final List<int[]> cuts = new ArrayList<int[]>();

		@Override
		protected void doEnumeration(Poset poset, VectorClock G) {
			cuts.add(Cuts.toArray(G));
		}

		@Override
		protected void doClose() { }
	}

	/**
	 * Builds a poset from events given in a linear extension order. Each event is
	 * {pid, c0, c1, ...} where c are the clocks of its vector clock (own clock
	 * equal to the 1-based index of the event on its chain), as in the poset files.
	 */
	public static DefaultPoset poset(int width, int[]... events) {
		DefaultPoset p = new DefaultPoset();
		Builder b = p.getBuilder();
		b.beginBuild(width);
		for (int[] e : events) {
			VectorClock vc = new VectorClock(width);
			for (int i = 0; i < width; ++i) vc.set(i, e[1 + i]);
			b.addVectorClock(e[0], vc);
		}
		b.endBuild();
		return p;
	}

	public static DefaultPoset parse(String file) throws Exception {
		DefaultPoset p = new DefaultPoset();
		new PosetParser().parsePosetFromFile(file, p.getBuilder());
		return p;
	}

	/**
	 * A random message-passing computation. Events are emitted in the order they
	 * were simulated, which is a linear extension of happened-before.
	 */
	public static DefaultPoset randomPoset(int width, int eventsPerProcess, double messageProbability, long seed) {
		Random rnd = new Random(seed);
		int[][] clock = new int[width][width];
		int[] remaining = new int[width];
		Arrays.fill(remaining, eventsPerProcess);
		List<List<int[]>> inbox = new ArrayList<List<int[]>>();
		for (int i = 0; i < width; ++i) inbox.add(new ArrayList<int[]>());
		List<int[]> events = new ArrayList<int[]>();
		int total = width * eventsPerProcess;
		while (total-- > 0) {
			int pid;
			do { pid = rnd.nextInt(width); } while (remaining[pid] == 0);
			--remaining[pid];
			List<int[]> mail = inbox.get(pid);
			if (!mail.isEmpty() && rnd.nextDouble() < 0.5) {
				int[] msg = mail.remove(rnd.nextInt(mail.size()));
				for (int i = 0; i < width; ++i) clock[pid][i] = Math.max(clock[pid][i], msg[i]);
			}
			++clock[pid][pid];
			if (width > 1 && rnd.nextDouble() < messageProbability) {
				int dst;
				do { dst = rnd.nextInt(width); } while (dst == pid);
				inbox.get(dst).add(clock[pid].clone());
			}
			int[] e = new int[width + 1];
			e[0] = pid;
			System.arraycopy(clock[pid], 0, e, 1, width);
			events.add(e);
		}
		return poset(width, events.toArray(new int[0][]));
	}

	/** Reference: every vector in the bounding box that isConsistentCut accepts, in lex order. */
	public static List<int[]> bruteForce(DefaultPoset poset) {
		int n = poset.width();
		int[] lo = new int[n], hi = new int[n];
		for (int i = 0; i < n; ++i) hi[i] = poset.sizeOfChain(i) - 1;
		return bruteForce(poset, lo, hi);
	}

	/** Same as {@link #bruteForce(DefaultPoset)} restricted to lo &lt;= G &lt;= hi. */
	public static List<int[]> bruteForce(DefaultPoset poset, int[] lo, int[] hi) {
		int n = poset.width();
		List<int[]> result = new ArrayList<int[]>();
		for (int i = 0; i < n; ++i) if (lo[i] > hi[i]) return result;
		int[] g = lo.clone();
		while (true) {
			if (poset.isConsistentCut(Cuts.toVectorClock(g))) result.add(g.clone());
			int k = n - 1;
			while (k >= 0 && g[k] == hi[k]) { g[k] = lo[k]; --k; }
			if (k < 0) break;
			++g[k];
		}
		return result;
	}

	/** Centralized LexTraverser output; the reference the distributed runs are compared with. */
	public static List<int[]> centralizedLex(DefaultPoset poset) {
		int n = poset.width();
		int[] hi = new int[n];
		for (int i = 0; i < n; ++i) hi[i] = poset.sizeOfChain(i) - 1;
		return lexInterval(poset, new int[n], hi);
	}

	/** Runs the existing LexTraverser over one interval, with the same guards the worker uses. */
	public static List<int[]> lexInterval(DefaultPoset poset, int[] lo, int[] hi) {
		CollectingEnumerator c = new CollectingEnumerator();
		LexTraverser lex = new LexTraverser();
		lex.setEnumerator(c);
		VectorClock l = Cuts.toVectorClock(lo), m = Cuts.toVectorClock(hi);
		if (l.equals(m)) c.enumerate(poset, l);
		else if (l.leq(m)) lex.boundedTraverse(poset, l, m);
		return c.cuts;
	}

	public static String show(List<int[]> cuts) {
		StringBuilder sb = new StringBuilder();
		for (int[] c : cuts) sb.append(Arrays.toString(c));
		return sb.toString();
	}

	/** Asserts the lists hold the same cuts in the same order, with a precise diagnostic. */
	public static void checkSameSequence(List<int[]> expected, List<int[]> actual, String what) {
		int n = Math.min(expected.size(), actual.size());
		for (int i = 0; i < n; ++i) {
			if (!Arrays.equals(expected.get(i), actual.get(i)))
				throw new AssertionError(what + ": first difference at position " + i + ": expected "
						+ Arrays.toString(expected.get(i)) + " but was " + Arrays.toString(actual.get(i)));
		}
		TestSuite.checkEquals(expected.size(), actual.size(), what + ": number of cuts");
	}

	/** Strictly increasing in lex order (so ordered and duplicate-free), all consistent. */
	public static void checkOrderedConsistentUnique(DefaultPoset poset, List<int[]> cuts, String what) {
		for (int i = 0; i < cuts.size(); ++i) {
			TestSuite.check(poset.isConsistentCut(Cuts.toVectorClock(cuts.get(i))),
					what + ": inconsistent cut " + Arrays.toString(cuts.get(i)));
			if (i > 0)
				TestSuite.check(Cuts.compare(cuts.get(i - 1), cuts.get(i)) < 0, what + ": cut "
						+ Arrays.toString(cuts.get(i)) + " does not follow " + Arrays.toString(cuts.get(i - 1))
						+ " in strictly increasing lex order");
		}
	}
}
