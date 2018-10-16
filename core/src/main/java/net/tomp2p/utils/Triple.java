package net.tomp2p.utils;

public class Triple<F, S, T> {
    private final F element0;
    private final S element1;
    private final T element2;

    public Triple(F element0, S element1, T element2) {
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


    public static <F, S, T> Triple<F, S, T> of(F element0, S element1, T element2) {
        return new Triple<F, S, T>(element0, element1, element2);
    }
    
    public static <F, S, T> Triple<F, S, T> create(F element0, S element1, T element2) {
		return new Triple<F, S, T>(element0, element1, element2);
	}

    @Override
    public int hashCode() {
        return (element0 == null ? 0 : element0.hashCode()) ^ (element1 == null ? 0 : element1.hashCode())
                ^ (element2 == null ? 0 : element2.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Triple)) {
            return false;
        }

        final Triple<?, ?, ?> other = (Triple<?, ?, ?>) o;
        return objectsEqual(other.element0, element0) && objectsEqual(other.element1, element1)
                && objectsEqual(other.element2, element2);
    }

    private static boolean objectsEqual(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    @Override
    public String toString() {
        return "Triple{" + String.valueOf(element0) + " " + String.valueOf(element1) + " " + String.valueOf(element2) + "}";
    }
}