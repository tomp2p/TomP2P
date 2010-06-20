package net.tomp2p.rpc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class DummyServer {

	private ServerBootstrap bootstrap;
	
	public DummyServer() {
		initServer();
		setHandler(new EchoHandler());
	}
	
	public DummyServer(ChannelHandler handler) {
		initServer();
		setHandler(handler);
	}
	
	private void setHandler(ChannelHandler handler) {
		bootstrap.getPipeline().addLast("handler", handler);
	}
	
	private void initServer() {
		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool());
		bootstrap = new ServerBootstrap(factory);
		//bootstrap.setPipelineFactory(new TomP2PServerPipelineFactory(new EmptyHandler()));
	}
	
	public void start() {
		bootstrap.bind(new InetSocketAddress("127.0.0.1", 8080));
	}
	
	public void stop() {
		//bootstrap.getPipeline().getChannel().unbind();
	}
	
	public ChannelPipeline getPipeline() {
		return bootstrap.getPipeline();
	}
	
	/**
	 * Prints every message it receives.
	 * @author Serge H�nni
	 *
	 */
	@ChannelPipelineCoverage("all")
	private class EchoHandler extends SimpleChannelHandler {
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			ChannelBuffer buf = (ChannelBuffer) e.getMessage();
			
			while(buf.readable())
				System.out.print((char) buf.readByte());
		}
	}
}
