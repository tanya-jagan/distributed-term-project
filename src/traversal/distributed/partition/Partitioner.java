package traversal.distributed.partition;

import java.util.List;

import poset.DefaultPoset;
import traversal.distributed.Task;

/**
 * Splits the lattice of consistent cuts into disjoint {@link Task} intervals.
 * Task ids must be 0..k-1 matching the list position.
 */
public interface Partitioner {

	String name();

	/**
	 * @param poset the poset to partition.
	 * @param targetTasks a hint for the desired number of tasks; a partitioner
	 *        with a fixed granularity (e.g. one task per event) may ignore it.
	 */
	List<Task> partition(DefaultPoset poset, int targetTasks);
}
