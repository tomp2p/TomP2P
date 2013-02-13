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
package net.tomp2p.message;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import static org.jboss.netty.channel.Channels.write;

@Sharable
public class TomP2PEncoderTCP implements ChannelDownstreamHandler {
    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (!(e instanceof MessageEvent)) {
            ctx.sendDownstream(e);
            return;
        }
        MessageEvent evt = (MessageEvent) e;
        Object msg = evt.getMessage();
        if (!(msg instanceof Message)) {
            ctx.sendDownstream(evt);
            return;
        }
        Message message = (Message) msg;
        final ChannelBuffer headerBuffer = ChannelBuffers.buffer(MessageCodec.HEADER_SIZE);
        MessageCodec.encodeHeader(headerBuffer, message);
        if (message.hasContent()) {
            ProtocolChunkedInput input = new ProtocolChunkedInput(ctx, message.getPrivateKey());
            input.copyToCurrent(headerBuffer);
            MessageCodec.encodePayload(message, input);
            write(ctx, e.getFuture(), input, evt.getRemoteAddress());
        } else {
            write(ctx, e.getFuture(), headerBuffer, evt.getRemoteAddress());
        }
    }
}