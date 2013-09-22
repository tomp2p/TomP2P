package net.tomp2p.futures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection2.ChannelCreator;

public abstract class FutureDHT<K extends BaseFuture> extends BaseFutureImpl<K> {

    // Stores futures of DHT operations, 6 is the maximum of futures being
    // generates as seen in Configurations (min.res + parr.diff)
    private final List<FutureResponse> requests = new ArrayList<FutureResponse>(6);

    private K self;

    /**
     * @param self2
     *            Set the type so that we are able to return it to the user. This is for making the API much more
     *            usable.
     */
    protected void self(final K self2) {
        this.self = self2;
    }

    /**
     * Returns back those futures that are still running. If 6 storage futures are started at the same time and 5 of
     * them finish, and we specified that we are fine if 5 finishes, then futureDHT returns success. However, the future
     * that may still be running is the one that stores the content to the closest peer. For testing this is not
     * acceptable, thus after waiting for futureDHT, one needs to wait for the running futures as well.
     * 
     * @return A future that finishes if all running futures are finished.
     */
    public FutureForkJoin<FutureResponse> getFutureRequests() {
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
        return self;
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
                getFutureRequests().addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
                    @Override
                    public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception {
                        channelCreator.shutdown();
                    }
                });
            }
        });
    }
}
