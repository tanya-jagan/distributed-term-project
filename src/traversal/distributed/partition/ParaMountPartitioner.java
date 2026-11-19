package traversal.distributed.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import poset.DefaultPoset;
import traversal.distributed.Cuts;
import traversal.distributed.Task;

/**
 * Uses the existing ParaMount partitioning: one task per poset node, with the
 * interval [getNodeLeastCut, getNodeBoundary] that
 * {@code ConcurrentUnorderedTraverser} hands to its threads.
 * <p>
 * These intervals are disjoint and cover the lattice, but they are NOT
 * contiguous in lexicographic order (cuts of different nodes interleave), so the
 * coordinator has to merge their outputs. Tasks are sorted by lower bound so
 * that the merge watermark advances as early as possible.
 */
public class ParaMountPartitioner implements Partitioner {
	private final boolean pruneEmpty;

	/** @param pruneEmpty drop intervals with lower &gt; upper, as the threaded traverser does. */
	public ParaMountPartitioner(boolean pruneEmpty) {
		this.pruneEmpty = pruneEmpty;
	}

	public ParaMountPartitioner() {
		this(true);
	}

	@Override
	public String name() {
		return "paramount";
	}

	@Override
	public List<Task> partition(DefaultPoset poset, int targetTasks) {
		List<Task> tasks = new ArrayList<Task>();
		for (int i = 0; i < poset.getNodeCount(); ++i) {
			Task t = new Task(i, Cuts.toArray(poset.getNodeLeastCut(i)), Cuts.toArray(poset.getNodeBoundary(i)));
			if (!pruneEmpty || !t.isEmpty()) tasks.add(t);
		}
		// stable sort: ties keep node order
		Collections.sort(tasks, new Comparator<Task>() {
			@Override
			public int compare(Task a, Task b) {
				return Cuts.compare(a.lower, b.lower);
			}
		});
		List<Task> result = new ArrayList<Task>(tasks.size());
		for (int i = 0; i < tasks.size(); ++i) result.add(tasks.get(i).withId(i));
		return result;
	}
}
