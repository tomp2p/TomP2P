package net.tomp2p.relay;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.SimpleBloomFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRouting extends DispatchHandler {
	
	private final static Logger logger = LoggerFactory.getLogger(RelayRouting.class);
	
	private final static byte ROUTING_TABLE_UPDATE = 33;
	private List<Map<Number160, PeerStatatistic>> peerMap = null;
	private final PeerAddress unreachablePeer;
	
	private final Peer peer;

	public RelayRouting(Peer peer, PeerAddress unreachablePeer) {
		super(peer.getPeerBean(), peer.getConnectionBean(), ROUTING_TABLE_UPDATE);
		peer.getConnectionBean().dispatcher().registerIoHandler(unreachablePeer.getPeerId(), this, NeighborRPC.NEIGHBORS_COMMAND);
		this.peer = peer;
		this.unreachablePeer = unreachablePeer;
		setObjectReply();
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
        if(peerMap == null) {
        	responder.response(createResponseMessage(message, Type.NOT_FOUND));
        }
		if (message.getKeyList().size() < 2) {
            throw new IllegalArgumentException("We need the location and domain key at least");
        }
        if (!(message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2
                || message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4)
                && (message.getCommand() == NeighborRPC.NEIGHBORS_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        Number160 locationKey = message.getKey(0);
        Number160 domainKey = message.getKey(1);
        Number320 locationAndDomainKey = new Number320(locationKey, domainKey);
        // Create response message and set neighbors
        final Message responseMessage = createResponseMessage(message, Type.OK);

        SortedSet<PeerAddress> neighbors = PeerMap.closePeers(unreachablePeer.getPeerId(), locationKey, NeighborRPC.NEIGHBOR_SIZE, peerMap);
        logger.debug("found the following neighbors {}", neighbors);
        NeighborSet neighborSet = new NeighborSet(NeighborRPC.NEIGHBOR_LIMIT, neighbors);
        responseMessage.setNeighborsSet(neighborSet);
        // check for fastget, -1 if, no domain provided, so we cannot
        // check content length, 0 for content not here , > 0 content here
        // int contentLength = -1;
        Number160 contentKey = message.getKey(2);
        SimpleBloomFilter<Number160> keyBloomFilter = message.getBloomFilter(0);
        SimpleBloomFilter<Number160> contentBloomFilter = message.getBloomFilter(1);
        // it is important to set an integer if a value is present
        boolean isDigest = message.getType() != Type.REQUEST_1;
        if (isDigest) {
            if (message.getType() == Type.REQUEST_2) {
                final DigestInfo digestInfo;
                if (contentKey != null) {
                    Number640 from = new Number640(locationAndDomainKey, contentKey, Number160.ZERO);
                    Number640 to = new Number640(locationAndDomainKey, contentKey, Number160.MAX_VALUE);
                    digestInfo = peerBean().storage().digest(from, to, -1, true);
                } else if (keyBloomFilter != null || contentBloomFilter != null) {
                    digestInfo = peerBean().storage().digest(locationAndDomainKey, keyBloomFilter,
                            contentBloomFilter, -1, true);
                } else {
                    Number640 from = new Number640(locationAndDomainKey, Number160.ZERO, Number160.ZERO);
                    Number640 to = new Number640(locationAndDomainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
                    digestInfo = peerBean().storage().digest(from, to, -1, true);
                }
                responseMessage.setInteger(digestInfo.getSize());
                responseMessage.setKey(digestInfo.getKeyDigest());
                responseMessage.setKey(digestInfo.getContentDigest());
            } else if (message.getType() == Type.REQUEST_3) {
                DigestInfo digestInfo = peerBean().trackerStorage().digest(locationKey, domainKey, null);
                if (digestInfo.getSize() == 0) {
                    logger.debug("No entry found on peer {}", message.getRecipient());
                }
                responseMessage.setInteger(digestInfo.getSize());
            }
        }
        responder.response(responseMessage);
	}
	
	private void setObjectReply() {
		peer.setObjectDataReply(new ObjectDataReply() {
			@SuppressWarnings("unchecked")
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if(request instanceof List<?>) {
					peerMap = (List<Map<Number160, PeerStatatistic>>) request;
					logger.trace("Peer map of unreachable peer {} was updated", sender);
				}
				return true;
			}
		});
	}

}
