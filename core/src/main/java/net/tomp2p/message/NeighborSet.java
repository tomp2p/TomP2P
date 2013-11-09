package net.tomp2p.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import net.tomp2p.peers.PeerAddress;

public class NeighborSet {
    private final int neighborLimit;
    private final Collection<PeerAddress> neighbors;

    public NeighborSet(int neighborLimit) {
        this.neighborLimit = neighborLimit;
        this.neighbors = new ArrayList<PeerAddress>();
    }

    public NeighborSet(final int neighborLimit, final Collection<PeerAddress> neighbors) {
        this.neighborLimit = neighborLimit;
        this.neighbors = neighbors;
        // remove neighbors that are over the limit
        int serializedSize = 1;
        // no need to cut if we don't provide a limit
        if (neighborLimit < 0) {
            return;
        }
        for (Iterator<PeerAddress> iterator = neighbors.iterator(); iterator.hasNext();) {
            PeerAddress neighbor = iterator.next();
            serializedSize += neighbor.size();
            if (serializedSize > neighborLimit) {
                iterator.remove();
                //System.err.println("remove "+ neighbor);
            } else {
                //System.err.println("keep "+ neighbor);
            }
        }
    }

    public Collection<PeerAddress> neighbors() {
        return neighbors;
    }

    public void addResult(Collection<PeerAddress> neighbors) {
        this.neighbors.addAll(neighbors);
    }

    public void add(PeerAddress neighbor) {
        this.neighbors.add(neighbor);
    }

    public int size() {
        return this.neighbors.size();
    }

    public int neighborLimit() {
        return neighborLimit;
    }
}
