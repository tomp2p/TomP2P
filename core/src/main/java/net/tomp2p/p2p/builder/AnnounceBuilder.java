package net.tomp2p.p2p.builder;

import java.net.InetAddress;
import java.util.Collection;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverResults;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureAnnounce;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.JobScheduler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

public class AnnounceBuilder implements Builder {
	
	private final Peer peer;
	
	private int port = Ports.DEFAULT_PORT;
	
	private JobScheduler jobScheduler = null;
	
	private boolean ping = false;
	
	private PeerAddress peerAddress = null;
	
	private int intervalMillis = 10000;
	
	private int repetitions = 5;
	
	private ConnectionConfiguration connectionConfiguration = null;
	
	public AnnounceBuilder(Peer peer) {
        this.peer = peer;
    }
	
	public int port() {
		return port;
	}

	public AnnounceBuilder port(int port) {
		this.port = port;
		return this;
	}
	
	public JobScheduler jobScheduler() {
		return jobScheduler;
	}

	public AnnounceBuilder jobScheduler(JobScheduler jobScheduler) {
		this.jobScheduler = jobScheduler;
		return this;
	}
	
	public PeerAddress peerAddress() {
		return peerAddress;
	}

	public AnnounceBuilder peerAddress(PeerAddress peerAddress) {
		this.peerAddress = peerAddress.changePeerId(Number160.ZERO);
		return this;
	}
	
	public AnnounceBuilder peerAddressOrig(PeerAddress peerAddress) {
		this.peerAddress = peerAddress;
		return this;
	}
	
	public AnnounceBuilder intervalMillis(int intervalMillis) {
		this.intervalMillis = intervalMillis;
		return this;
	}
	
	public int intervalMillis() {
		return intervalMillis;
	}
	
	public AnnounceBuilder repetitions(int repetitions) {
		this.repetitions = repetitions;
		return this;
	}
	
	public int repetitions() {
		return repetitions;
	}
	
	public AnnounceBuilder ping() {    
	    return this;
    }
	
	public AnnounceBuilder setPing(boolean ping) {
		this.ping = ping;
	    return this;
    }
	
	public boolean isPing() {    
	    return ping;
    }
	
    public FutureAnnounce start() {
    	
    	if (connectionConfiguration == null) {
            connectionConfiguration = new DefaultConnectionConfiguration();
        }
    	
    	FutureAnnounce futureAnnounce = new FutureAnnounce();
    	futureAnnounce = ping ? pingAnnounce(futureAnnounce) : announce(futureAnnounce);
    	if(jobScheduler != null) {
    		jobScheduler.start(this, intervalMillis, repetitions);
    	}
    	return futureAnnounce;
    }
    
    private FutureAnnounce pingAnnounce(final FutureAnnounce futureAnnounce) {
    	FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
    	Utils.addReleaseListener(fcc, futureAnnounce);
    	fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
    		@Override
            public void operationComplete(FutureChannelCreator future) throws Exception {
    			if(future.isSuccess()) {
    				FutureResponse futureResponse = peer.announceRPC().ping(peerAddress, connectionConfiguration, future.channelCreator());
    				futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
                        public void operationComplete(FutureResponse future) throws Exception {
	                        if(future.isSuccess()) {
	                        	futureAnnounce.done();
	                        } else {
	                        	futureAnnounce.failed(future);
	                        }
                        }
					});
    			} else {
    				futureAnnounce.failed(future);
    			}
    		}
        });
    	return futureAnnounce;
    }
    
    private FutureAnnounce announce(final FutureAnnounce futureAnnounce) {
    	final DiscoverResults discoverResults = peer.connectionBean().channelServer().discoverNetworks().currentDiscoverResults();
        final Collection<InetAddress> broadcastAddresses = discoverResults.existingBroadcastAddresses();
        final int size = broadcastAddresses.size();
        final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(size);
        if (size > 0) {
            FutureChannelCreator fcc = peer.connectionBean().reservation().create(size, 0);
            Utils.addReleaseListener(fcc, futureAnnounce);
            fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        addPingListener(futureAnnounce, futureLateJoin);
                        for (InetAddress broadcastAddress: broadcastAddresses) {
                            final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress,
                                    port, port);
                            FutureResponse validBroadcast = peer.announceRPC().broadcast(
                                    peerAddress, connectionConfiguration, future.channelCreator());
                            futureLateJoin.add(validBroadcast);
                        }
                    } else {
                    	futureAnnounce.failed(future);
                    }
                }
            });
        } else {
        	futureAnnounce.failed("No broadcast address found. Cannot ping nothing");
        }
        return futureAnnounce;
    }

	private void addPingListener(final FutureAnnounce futureAnnounce, FutureLateJoin<FutureResponse> futureLateJoin) {
       	//we have one successful reply
    	futureLateJoin.addListener(new BaseFutureAdapter<FutureLateJoin<FutureResponse>>() {
        	@Override
            public void operationComplete(FutureLateJoin<FutureResponse> future) throws Exception {
                if(future.futuresDone().size() > 0) {
                	futureAnnounce.done();
                } else {
                	futureAnnounce.failed(future);
                }       
            }
        });
    }
}
