package net.tomp2p.dht;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;

public abstract class FutureDHT<K extends BaseFuture> extends BaseFutureImpl<K> {

    // Stores futures of DHT operations, 6 is the maximum of futures being
    // generates as seen in Configurations (min.res + parr.diff)
    private final List<FutureResponse> requests = new ArrayList<FutureResponse>(6);

    // A reference to the builder that contains the data we were looking for
    private final DHTBuilder<?> builder;

    // A reference to the routing process that run before the DHT operations
    private FutureRouting futureRouting;

    public FutureDHT(DHTBuilder<?> builder) {
        this.builder = builder;
    }

    /**
     * @return A reference to the builder that contains the data we were looking for
     */
    public DHTBuilder<?> builder() {
        return builder;
    }

    /**
     * Returns back those futures that are still running. If 6 storage futures are started at the same time and 5 of
     * them finish, and we specified that we are fine if 5 finishes, then futureDHT returns success. However, the future
     * that may still be running is the one that stores the content to the closest peer. For testing this is not
     * acceptable, thus after waiting for futureDHT, one needs to wait for the running futures as well.
     * 
     * @return A future that finishes if all running futures are finished.
     */
    public FutureForkJoin<FutureResponse> futureRequests() {
        synchronized (lock) {
            final int size = requests.size();
            final FutureResponse[] futureResponses = new FutureResponse[size];

            for (int i = 0; i < size; i++) {
                futureResponses[i] = requests.get(i);
            }
            return new FutureForkJoin<FutureResponse>(new AtomicReferenceArray<FutureResponse>(
                    futureResponses));
        }
    }

    /**
     * Adds all requests that have been created for the DHT operations. Those were created after the routing process.
     * 
     * @param futureResponse
     *            The futurRepsonse that has been created
     */
    public K addRequests(final FutureResponse futureResponse) {
        synchronized (lock) {
            requests.add(futureResponse);
        }
        return self();
    }

    /**
     * Adds a listener to the response future and releases all aquired channels in channel creator.
     * 
     * @param channelCreator
     *            The channel creator that will be shutdown and all connections will be closed
     */
    public void addFutureDHTReleaseListener(final ChannelCreator channelCreator) {
        addListener(new BaseFutureAdapter<FutureDHT<K>>() {
            @Override
            public void operationComplete(final FutureDHT<K> future) throws Exception {
                futureRequests().addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
                    @Override
                    public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception {
                        channelCreator.shutdown();
                    }
                });
            }
        });
    }

    /**
     * Returns the future object that was used for the routing. Before the FutureDHT is used, FutureRouting has to be
     * completed successfully.
     * 
     * @return The future object during the previous routing, or null if routing failed completely.
     */
    public FutureRouting futureRouting() {
        synchronized (lock) {
            return futureRouting;
        }
    }

    /**
     * Sets the future object that was used for the routing. Before the FutureDHT is used, FutureRouting has to be
     * completed successfully.
     * 
     * @param futureRouting
     *            The future object to set
     */
    public void futureRouting(final FutureRouting futureRouting) {
        synchronized (lock) {
            this.futureRouting = futureRouting;
        }
    }
}
