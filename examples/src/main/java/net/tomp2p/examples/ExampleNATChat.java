package net.tomp2p.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Random;

import net.tomp2p.connection2.Bindings;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageDisk;

public class ExampleNATChat
{
    private static int keyStore = 543453049;
    private static int serverPort = 4000;
    private static int clientPort = 4563;

    public void startServer(String addr) throws Exception 
    {
        Peer peer = null;
        try 
        {
            Random r = new Random(42L);         
            //peer.getP2PConfiguration().setBehindFirewall(true);
            Bindings b = new Bindings();
            //b.addInterface("eth0");
            b.addAddress(InetAddress.getByName(addr));
            //b.addAddress(InetAddress.getByAddress(addr));
            peer = new PeerMaker(
                    new Number160(r)).setBindings(b).setPorts(serverPort).makeAndListen();
            peer.getPeerBean().setStorage(new StorageDisk("storage"));
            System.out.println("peer started.");
            for (;;) 
            {
                Thread.sleep(5000);
                FutureDHT fd = peer.get(new Number160(keyStore)).setAll().start();
                fd.awaitUninterruptibly();
                int size = fd.getDataMap().size();
                System.out.println("size " + size);
                Iterator<Data> iterator = fd.getDataMap().values().iterator();
                while( iterator.hasNext() )
                {
                    Data d = iterator.next();
                     System.out.println("got: " + d.getObject().toString());
                }
            }
        }
        finally 
        {
            //peer.halt();
        }
    }
    
    public static void main(String[] args) throws Exception 
    {
        if(args.length>1 && args[0].equals("bootstrap"))
        {
            ExampleNATChat t = new ExampleNATChat();
            t.startServer(args[1]);
        }
        else if (args.length > 0)
        {
            startClientNAT(args[0]);
        }
        else
        {
            System.err.println("Must be called with:"+
                    "\n\tNATChat bootstrap <ip>\t\t(supernode)\n\t"+
                    "NATChat <bootstrap ip>\t\t(normal node).\n");
        }
    }
    
    public static void startClientNAT(String ip) throws Exception 
    {
        Random r = new Random(43L);
        Peer peer = new PeerMaker(new Number160(r)).setPorts(clientPort).makeAndListen();
        peer.getConfiguration().setBehindFirewall(true);
        PeerAddress bootStrapServer = new PeerAddress(Number160.ZERO,
                InetAddress.getByName(ip), serverPort, serverPort);
        FutureDiscover fd = peer.discover().setPeerAddress(bootStrapServer).start();
        System.out.println("About to wait...");
        fd.awaitUninterruptibly();
        if (fd.isSuccess()) 
        {
            System.out.println("*** FOUND THAT MY OUTSIDE ADDRESS IS "
                    + fd.getPeerAddress());
        } 
        else 
        {
            System.out.println("*** FAILED " + fd.getFailedReason());
        }

        bootStrapServer = fd.getReporter(); 
        FutureBootstrap bootstrap = peer.bootstrap().setPeerAddress(bootStrapServer).start();
        bootstrap.awaitUninterruptibly();
        if (!bootstrap.isSuccess()) {
            System.out.println("*** COULD NOT BOOTSTRAP!");
        }
        else
        {
            System.out.println("*** SUCCESSFUL BOOTSTRAP");
        }

        String inLine = null;
        while ( (inLine = getLine()) != null)
        {
            if (inLine.equals("show"))
            {
                FutureDHT fdht = peer.get(new Number160(keyStore)).setAll().start();
                fdht.awaitUninterruptibly();
                Iterator<Data> iterator = fdht.getDataMap().values().iterator();
                StringBuffer allString = new StringBuffer();
                FutureDHT fg;
                while( iterator.hasNext() )
                {
                    Data d = iterator.next();
                    fg = peer.get(new Number160(((Integer)d.getObject()).intValue())).start();
                    fg.awaitUninterruptibly();
                    if (fg.getData() != null)
                    {
                        allString.append(fg.getData().getObject().toString()).append("\n");
                    }
                    else
                    {
                        System.err.println("Could not find key for val: " + d.getObject());
                    }
                }
                System.out.println("got: " + allString.toString());
            }
            else
            {
                int r2 = new Random().nextInt();            
                System.out.println("Storing DHT address (" + r2 + ") in DHT");
                peer.add(new Number160(keyStore)).setData(new Data(r2)).start().awaitUninterruptibly();
                System.out.println("Adding (" + inLine + ") to DHT");
                peer.put(new Number160(r2)).setData(new Data(inLine)).start().awaitUninterruptibly();
            }
        }
        System.out.println("Shutting down...");
        //peer.halt();
    }



    static String getLine() 
    {
        System.out.print("Please enter a short line of text: ");
        InputStreamReader converter = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(converter); 
        String inLine="";
        try {
            inLine = in.readLine();
        }
        catch(Exception e)
        {
            System.err.println("Error reading input.");
            e.printStackTrace();
            System.exit(1);
        }
        return inLine;
    }
}
