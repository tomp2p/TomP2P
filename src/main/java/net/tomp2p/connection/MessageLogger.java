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
package net.tomp2p.connection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;

import net.tomp2p.message.Message;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prints the received message to a gzip encoded file. We omit to also store the
 * send message because it will be the same as the received:
 * 
 * <pre>
 * peer A -> messsage request -> peer B
 * peer B -> message reply -> peer A
 * 
 * peer A (send request)
 * peer B (receive request) -> will be logged
 * peer B (send reply)
 * peer A (receive reply) -> will be logged
 * </pre>
 * 
 * @author Thomas Bocek
 */
@Sharable
public class MessageLogger implements ChannelUpstreamHandler {
    final private static Logger logger = LoggerFactory.getLogger(MessageLogger.class);

    final private PrintWriter pw;

    final private GZIPOutputStream gz;

    private boolean open = false;

    /**
     * Creates a new message logger that outputs the received messages in a
     * gzipped file.
     * 
     * @param outputFile
     *            The output file
     * @throws FileNotFoundException
     * @throws IOException
     */
    public MessageLogger(File outputFile) throws FileNotFoundException, IOException {
        this.gz = new GZIPOutputStream(new FileOutputStream(outputFile));
        this.pw = new PrintWriter(gz);
        open = true;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof MessageEvent)
            messageReceived((MessageEvent) e);
        ctx.sendUpstream(e);
    }

    /**
     * Prints out custom messages. This is useful for markers, e.g. from when to
     * start logging or when a certain event happend. The custom message will
     * have "C:" at the beginning.
     * 
     * @param customMessage
     *            The custom message
     */
    public void customMessage(String customMessage) {
        synchronized (pw) {
            if (open) {
                pw.println("C:".concat(customMessage));
                pw.flush();
            }
        }
    }

    /**
     * Prints out the message in a digest form. The received message will have
     * "R:" at the beginning.
     * 
     * @param e
     *            The message event from Netty
     */
    private void messageReceived(MessageEvent e) {
        synchronized (pw) {
            if (open && e.getMessage() instanceof Message) {
                pw.println("R:".concat(e.getMessage().toString()));
                pw.flush();
            }
        }
    }

    /**
     * Shutdown the stream. Once is closed, it cannot be opened again.
     */
    public void shutdown() {
        synchronized (pw) {
            pw.flush();
            try {
                gz.finish();
                gz.close();
            } catch (IOException e) {
                // try hard, otherwise we cannot do anything...
                logger.error(e.toString());
                e.printStackTrace();
            }
            pw.close();
            open = false;
        }
    }
}
