package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;

public class DummyCoder
{
    final private SocketAddress sockRemote = new InetSocketAddress( 2000 );

    final private SocketAddress sockLocal = new InetSocketAddress( 1000 );

    final private DummyChannel dc = new DummyChannel( sockRemote, sockLocal );

    final private DummyChannelHandlerContext dchc = new DummyChannelHandlerContext( dc );

    final private boolean tcp;

    public DummyCoder( boolean tcp )
    {
        this.tcp = tcp;
    }

    public ChannelBuffer encode( final Message m1 )
        throws Exception
    {
        MessageEvent e = new MessageEvent()
        {

            @Override
            public ChannelFuture getFuture()
            {
                return new ChannelFuture()
                {

                    @Override
                    public boolean setSuccess()
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public boolean setProgress( long amount, long current, long total )
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public boolean setFailure( Throwable cause )
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public void removeListener( ChannelFutureListener listener )
                    {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public boolean isSuccess()
                    {
                        return true;
                    }

                    @Override
                    public boolean isDone()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCancelled()
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public Channel getChannel()
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Throwable getCause()
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public boolean cancel()
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly( long timeout, TimeUnit unit )
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly( long timeoutMillis )
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public ChannelFuture awaitUninterruptibly()
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public boolean await( long timeout, TimeUnit unit )
                        throws InterruptedException
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public boolean await( long timeoutMillis )
                        throws InterruptedException
                    {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public ChannelFuture await()
                        throws InterruptedException
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public void addListener( ChannelFutureListener listener )
                    {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public ChannelFuture rethrowIfFailed()
                        throws Exception
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public ChannelFuture sync()
                        throws InterruptedException
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public ChannelFuture syncUninterruptibly()
                    {
                        // TODO Auto-generated method stub
                        return null;
                    }
                };
            }

            @Override
            public Channel getChannel()
            {
                return null;
            }

            @Override
            public SocketAddress getRemoteAddress()
            {
                return null;
            }

            @Override
            public Object getMessage()
            {
                return m1;
            }
        };
        new TomP2PEncoderTCP().handleDownstream( dchc, e );

        // decode
        ChannelBuffer flat = ChannelBuffers.dynamicBuffer();
        Object obj = dchc.getInput();
        if ( obj instanceof ProtocolChunkedInput )
        {
            ProtocolChunkedInput p = (ProtocolChunkedInput) obj;
            while ( p.hasNextChunk() )
            {
                flat.writeBytes( (ChannelBuffer) p.nextChunk() );
            }
            return flat;
        }
        else
            return (ChannelBuffer) obj;
    }

    public Message decode( ChannelBuffer buffer )
        throws Exception
    {
        if ( tcp )
            return (Message) new TomP2PDecoderTCP().decode( null, dc, buffer );
        else
            return (Message) new TomP2PDecoderUDP().decode( null, dc, buffer, new InetSocketAddress( "127.0.0.1", 1111 ) );
    }
}
