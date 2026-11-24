package traversal.distributed;

import java.util.Arrays;

/**
 * A work assignment: enumerate every consistent cut G with lower &lt;= G &lt;= upper
 * (component-wise) using bounded lexicographic enumeration.
 * <p>
 * Contract used by the coordinator's merge: when the interval is non-empty,
 * {@code lower} is itself a consistent cut, hence the lexicographically smallest
 * cut of the task.
 */
public final class Task {
	public final int id;
	public final int[] lower;
	public final int[] upper;

	public Task(int id, int[] lower, int[] upper) {
		this.id = id;
		this.lower = lower;
		this.upper = upper;
	}

	/** True when no cut can lie between the bounds. */
	public boolean isEmpty() {
		for (int i = 0; i < lower.length; ++i)
			if (lower[i] > upper[i]) return true;
		return false;
	}

	public Task withId(int newId) {
		return new Task(newId, lower, upper);
	}

	@Override
	public String toString() {
		return "Task#" + id + Arrays.toString(lower) + ".." + Arrays.toString(upper);
	}
}
