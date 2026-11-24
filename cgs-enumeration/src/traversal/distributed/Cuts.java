package traversal.distributed;

import poset.VectorClock;

/**
 * Helpers for cuts that are stored as plain int arrays (the wire/merge
 * representation): lexicographic comparison and an order-dependent hash.
 */
public final class Cuts {
	private Cuts() { }

	private static final long SEQ_MULT = 0x9E3779B97F4A7C15L; // odd, so it is invertible mod 2^64
	private static final long FNV_PRIME = 0x100000001B3L;
	private static final long FNV_BASIS = 0xCBF29CE484222325L;

	/** Lexicographic comparison of two cuts of n clocks each. */
	public static int compare(int[] a, int aOff, int[] b, int bOff, int n) {
		for (int i = 0; i < n; ++i) {
			int x = a[aOff + i], y = b[bOff + i];
			if (x != y) return x < y ? -1 : 1;
		}
		return 0;
	}

	public static int compare(int[] a, int[] b) {
		return compare(a, 0, b, 0, a.length);
	}

	public static int[] toArray(VectorClock vc) {
		int[] a = new int[vc.size()];
		for (int i = 0; i < a.length; ++i) a[i] = vc.get(i);
		return a;
	}

	public static VectorClock toVectorClock(int[] a) {
		VectorClock vc = new VectorClock(a.length);
		for (int i = 0; i < a.length; ++i) vc.set(i, a[i]);
		return vc;
	}

	/** Mixes the clocks of one cut into a 64-bit value. */
	public static long cutValue(int[] a, int off, int n) {
		long x = FNV_BASIS;
		for (int i = 0; i < n; ++i) x = (x ^ (a[off + i] + 1L)) * FNV_PRIME;
		return mix(x);
	}

	public static long cutValue(VectorClock g) {
		long x = FNV_BASIS;
		for (int i = 0, n = g.size(); i < n; ++i) x = (x ^ (g.get(i) + 1L)) * FNV_PRIME;
		return mix(x);
	}

	private static long mix(long x) {
		x ^= x >>> 33;
		x *= 0xFF51AFD7ED558CCDL;
		x ^= x >>> 33;
		return x;
	}

	/** Appends one cut value to a running sequence hash. */
	public static long append(long hash, long cutValue) {
		return hash * SEQ_MULT + cutValue;
	}

	/**
	 * Hash of the concatenation of two sequences: H(A + B) = H(A) * P^|B| + H(B).
	 */
	public static long concat(long hashA, long hashB, long lengthB) {
		return hashA * pow(SEQ_MULT, lengthB) + hashB;
	}

	private static long pow(long base, long exp) {
		long result = 1;
		while (exp > 0) {
			if ((exp & 1) != 0) result *= base;
			base *= base;
			exp >>= 1;
		}
		return result;
	}
}
