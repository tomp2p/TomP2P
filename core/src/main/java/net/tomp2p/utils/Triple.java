package net.tomp2p.utils;

public class Triple<F, S, T> {
    public final F first;
    public final S second;
    public final T third;

    public Triple(F first, S second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
    
    public static <F, S, T> Triple<F, S, T> create(F first, S second, T third) {
		return new Triple<F, S, T>(first, second, third);
	}

    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode())
                ^ (third == null ? 0 : third.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Triple)) {
            return false;
        }

        final Triple<?, ?, ?> other = (Triple<?, ?, ?>) o;
        return objectsEqual(other.first, first) && objectsEqual(other.second, second)
                && objectsEqual(other.third, third);
    }

    private static boolean objectsEqual(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    @Override
    public String toString() {
        return "Triple{" + String.valueOf(first) + " " + String.valueOf(second) + " " + String.valueOf(third) + "}";
    }
}