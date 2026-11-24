package traversal.distributed;

/**
 * Entry point of the test suite: {@code java traversal.distributed.AllTests [nameFilter]}.
 * Large-input tests need {@code -Dtest.large=true}.
 */
public class AllTests {
	public static void main(String[] args) {
		TestSuite suite = new TestSuite();
		PartitionTests.register(suite);
		MergerTests.register(suite);
		DistributedTests.register(suite);
		int failures = suite.runAll(args.length > 0 ? args[0] : null);
		System.exit(failures == 0 ? 0 : 1);
	}
}
