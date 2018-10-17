package net.tomp2p.utils;

public class Triple<F, S, T> {

    public final static Triple<?,?,?> EMPTY = empty();

    private final F element0;
    private final S element1;
    private final T element2;

    public Triple(final F element0, final S element1, final T element2) {
        this.element0 = element0;
        this.element1 = element1;
        this.element2 = element2;
    }

    public F e0() {
        return element0;
    }

    public S e1() {
        return element1;
    }

    public T e2() {
        return element2;
    }

    public F element0() {
        return element0;
    }

    public S element1() {
        return element1;
    }

    public T element2() {
        return element2;
    }


    public static <F, S, T> Triple<F, S, T> of(
            final F element0, final S element1, final T element2) {
        return new Triple<F, S, T>(element0, element1, element2);
    }
    
    public static <F, S, T> Triple<F, S, T> create(
            final F element0, final S element1, final T element2) {
		return new Triple<F, S, T>(element0, element1, element2);
	}

	public boolean isEmpty() {
        return this == EMPTY || (element0 == null && element1 == null && element2 == null);
    }

    public static <F, S, T> Triple<F, S, T> empty() {
        return new Triple<>(null, null, null);
    }


    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Triple)) {
            return false;
        }
        if (this == o) {
            return true;
        }

        final Triple<?, ?, ?> other = (Triple<?, ?, ?>) o;
        return objectsEqual(other.element0, element0) && objectsEqual(other.element1, element1)
                && objectsEqual(other.element2, element2);
    }

    @Override
    public int hashCode() {
        return (element0 == null ? 0 : element0.hashCode()) ^ (element1 == null ? 0 : element1.hashCode())
                ^ (element2 == null ? 0 : element2.hashCode());
    }

    private static boolean objectsEqual(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Triple{");
        return sb.append(String.valueOf(element0))
                .append(',')
                .append(String.valueOf(element0))
                .append(',')
                .append(String.valueOf(element2))
                .append('}').toString();
    }
}