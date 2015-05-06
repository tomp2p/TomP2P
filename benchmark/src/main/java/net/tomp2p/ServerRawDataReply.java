package net.tomp2p;

import net.tomp2p.message.Buffer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;

public class ServerRawDataReply implements RawDataReply {
    private int _counter;

    public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete)
    {
        System.out.println(String.format("%s.", ++_counter));
        System.out.println(String.format("Request from %s.", sender));
        System.out.println(String.format("Buffer Size : %s", requestBuffer.length()));

        // server returns just OK if same buffer is returned
        return requestBuffer;
    }
}