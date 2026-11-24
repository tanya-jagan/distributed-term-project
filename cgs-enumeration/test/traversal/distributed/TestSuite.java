package traversal.distributed;

import java.util.ArrayList;
import java.util.List;

/** Minimal dependency-free test runner (the project ships no JUnit jar). */
public class TestSuite {

	public interface TestBody {
		void run() throws Exception;
	}

	private static class Case {
		final String name;
		final boolean slow;
		final TestBody body;

		Case(String name, boolean slow, TestBody body) {
			this.name = name;
			this.slow = slow;
			this.body = body;
		}
	}

	private final List<Case> cases = new ArrayList<Case>();

	public void add(String name, TestBody body) {
		cases.add(new Case(name, false, body));
	}

	/** A test that only runs with -Dtest.large=true. */
	public void addSlow(String name, TestBody body) {
		cases.add(new Case(name, true, body));
	}

	/** @return the number of failures. */
	public int runAll(String filter) {
		boolean large = Boolean.getBoolean("test.large");
		int passed = 0, failed = 0, skipped = 0;
		long start = System.nanoTime();
		for (Case c : cases) {
			if (filter != null && !c.name.contains(filter)) continue;
			if (c.slow && !large) {
				++skipped;
				System.out.println("SKIP  " + c.name + "  (needs -Dtest.large=true)");
				continue;
			}
			long t0 = System.nanoTime();
			try {
				c.body.run();
				++passed;
				System.out.printf("PASS  %s  (%d ms)%n", c.name, (System.nanoTime() - t0) / 1000000);
			} catch (Throwable t) {
				++failed;
				System.out.printf("FAIL  %s  (%d ms)%n", c.name, (System.nanoTime() - t0) / 1000000);
				t.printStackTrace(System.out);
			}
		}
		System.out.printf("%n%d passed, %d failed, %d skipped in %d ms%n", passed, failed, skipped,
				(System.nanoTime() - start) / 1000000);
		return failed;
	}

	public static void check(boolean condition, String message) {
		if (!condition) throw new AssertionError(message);
	}

	public static void checkEquals(Object expected, Object actual, String message) {
		if (expected == null ? actual != null : !expected.equals(actual))
			throw new AssertionError(message + ": expected <" + expected + "> but was <" + actual + ">");
	}
}
