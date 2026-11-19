package traversal.distributed;

import poset.Poset;
import poset.VectorClock;
import traversal.enumerator.AbstractEnumerator;

/**
 * Sequential enumerator that records the count, an order-dependent hash and
 * whether the cuts arrived strictly increasing in lex order. Used to produce the
 * reference digest of the centralized traversers, and as the coordinator's sink
 * to compare against.
 */
public class DigestEnumerator extends AbstractEnumerator {
	private long count = 0;
	private long hash = 0;
	private long orderViolations = 0;
	private int[] previous;

	@Override
	protected void doEnumeration(Poset poset, VectorClock G) {
		++count;
		hash = Cuts.append(hash, Cuts.cutValue(G));
		if (previous == null) {
			previous = new int[G.size()];
		} else {
			for (int i = 0; i < previous.length; ++i) {
				int a = previous[i], b = G.get(i);
				if (a != b) {
					if (a > b) ++orderViolations;
					break;
				}
				if (i == previous.length - 1) ++orderViolations; // equal to the previous cut
			}
		}
		for (int i = 0; i < previous.length; ++i) previous[i] = G.get(i);
	}

	@Override
	protected void doClose() { }

	public long count() { return count; }

	public long digest() { return hash; }

	/** Number of cuts that were not strictly greater than their predecessor. */
	public long orderViolations() { return orderViolations; }
}
