/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.p2p;

public class RoutingConfiguration {
    final private int maxDirectHits;

    final private int maxNoNewInfoDiff;

    final private int maxFailures;

    final private int maxSuccess;

    final private int parallel;

    final private boolean forceTCP;

    public RoutingConfiguration(int maxNoNewInfoDiff, int maxFailures, int parallel) {
        this(Integer.MAX_VALUE, maxNoNewInfoDiff, maxFailures, 20, parallel);
    }

    public RoutingConfiguration(int maxNoNewInfoDiff, int maxFailures, int maxSuccess, int parallel) {
        this(Integer.MAX_VALUE, maxNoNewInfoDiff, maxFailures, maxSuccess, parallel);
    }

    public RoutingConfiguration(int directHits, int maxNoNewInfoDiff, int maxFailures, int maxSuccess, int parallel) {
        this(directHits, maxNoNewInfoDiff, maxFailures, maxSuccess, parallel, false);
    }

    /**
     * Sets the routing configuration and its stop conditions.
     * 
     * @param maxDirectHits
     *            Number of direct hits (d): This is used for fetching data. If d peers have been contacted that have
     *            the data stored, routing stops.
     * @param maxNoNewInfoDiff
     *            Number of no new information (n): This is mainly used for storing data. It searches the closest peers
     *            and if n peers do not report any closer nodes, the routing stops.
     * @param maxFailures
     *            Number of failures (f): The routing stops if f peers fail to respond.
     * @param maxSuccess
     *            Number of success (s): The routing stops if s peers respond.
     * @param parallel
     *            Number of parallel requests (p): This tells the routing how many peers to contact in parallel.
     * @param forceTCP
     *            Flag to indicate that routing should be done with TCP instead of UDP
     */
    public RoutingConfiguration(final int maxDirectHits, final int maxNoNewInfoDiff, final int maxFailures,
            final int maxSuccess, final int parallel, final boolean forceTCP) {
        if (maxDirectHits < 0 || maxNoNewInfoDiff < 0 || maxFailures < 0 || parallel < 0) {
            throw new IllegalArgumentException("need to be larger or equals zero");
        }
        this.maxDirectHits = maxDirectHits;
        this.maxNoNewInfoDiff = maxNoNewInfoDiff;
        this.maxFailures = maxFailures;
        this.maxSuccess = maxSuccess;
        this.parallel = parallel;
        this.forceTCP = forceTCP;
    }

    public int getMaxDirectHits() {
        return maxDirectHits;
    }

    /**
     * This returns the difference to the min value of P2P configuration. We need to have a difference, because we need
     * to search at least for min peers in the routing, as otherwise if we find the closest node by chance, then we
     * don't reach min.
     * 
     * @return
     */
    public int getMaxNoNewInfoDiff() {
        return maxNoNewInfoDiff;
    }

    public int getMaxNoNewInfo(int minimumResults) {
        return maxNoNewInfoDiff + minimumResults;
    }

    public int getMaxFailures() {
        return maxFailures;
    }

    public int getMaxSuccess() {
        return maxSuccess;
    }

    public int getParallel() {
        return parallel;
    }

    /**
     * @return True if the routing should use TCP instead of the default UDP
     */
    public boolean isForceTCP() {
        return forceTCP;
    }
}
