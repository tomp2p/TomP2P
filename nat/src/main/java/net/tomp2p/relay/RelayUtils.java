package net.tomp2p.relay;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;

public class RelayUtils {
	
	public static Buffer encodeMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		Encoder e = new Encoder(null);
		CompositeByteBuf buf = Unpooled.compositeBuffer();
		e.write(buf, message);
		return new Buffer(buf);
	}
	
	public static Message decodeMessage(Buffer buf, InetSocketAddress recipient, InetSocketAddress sender) throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
		Decoder d = new Decoder(null);
        d.decodeHeader(buf.buffer(), recipient, sender);
        d.decodePayload(buf.buffer());
        return d.message();
	}

}
