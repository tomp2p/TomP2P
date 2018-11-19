package net.tomp2p.message;

import lombok.*;
import lombok.experimental.Accessors;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import org.whispersystems.curve25519.Curve25519KeyPair;

@Builder
@RequiredArgsConstructor
@Accessors(fluent = true, chain = true)
public class MessageHeader {
    @Getter private final PeerAddress serverPeerAddress;
    @Getter private final int version;
    @Getter private final int messageId;
    @Getter private final PeerAddress recipient;
    @Getter private final Number256 senderId;
    @Getter private final byte[] privateKey;
}
