package net.tomp2p.p2p;

import java.util.Comparator;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatistic;

/**
 * A sample class class used by the routing mechanism to prioritise peers with
 * a low latency during routing.
 *
 * First sorting criteria is bit distance measured in equal high-bits. Closer peers
 * are prioritised.
 *
 * Second sorting criteria is the availability of RTT information. Peers with RTT
 * measurements are prioritised.
 *
 * Third sorting criteria is the average RTT. Faster responding peers are prioritised.
 *
 * The last tie-breaker is the complete XOR distance.
 *
 * It is important to never let the comparator return 0 for non-equal peer addresses,
 * as this would conflict with the usage of the comparator in the TreeSet, where
 * the comparator returning 0 means that both entries are equal.
 *
 * Created by Sebastian Stephan on 06.12.14.
 */
public class RTTPeerStatisticComparator implements PeerStatisticComparator {
    @Override
    public Comparator<PeerStatistic> getComparator(final Number160 location) {
        return new Comparator<PeerStatistic>() {
            @Override
            public int compare(PeerStatistic o1, PeerStatistic o2) {
                // Ensure consistency with equals
                if (o1.peerAddress().equals(o2.peerAddress())) return 0;

                // First sort by bucket distance
                int firstComp = PeerMap.classCloser(location, o1.peerAddress(), o2.peerAddress());
                if (firstComp == 0) {
                    // Second sort by "is rtt available"
                    if (o1.getMeanRTT() < 0 && o2.getMeanRTT() >= 0) {
                        return 1;
                    } else if (o1.getMeanRTT() >= 0 && o2.getMeanRTT() < 0) {
                        return -1;
                    } else if (o1.getMeanRTT() >= 0 && o2.getMeanRTT() >= 0 && o1.getMeanRTT() != o2.getMeanRTT()) {
                        // Third sort by rtt if they are different)
                        Long rtt1 = o1.getMeanRTT();
                        Long rtt2 = o2.getMeanRTT();
                        return rtt1.compareTo(rtt2);
                    }
                    // Last sort by true xor distance (will never be 0)
                    return PeerMap.isKadCloser(location, o1.peerAddress(), o2.peerAddress());
                } else {
                    return firstComp;
                }
            }
        };
    }
}
