package net.tomp2p.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import junit.framework.Assert;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.StorageGeneric.PutStatus;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStorage
{
    final private static Number160 locationKey = new Number160( 10 );

    final private static Number160 domainKey = new Number160( 20 );

    final private static Number160 content1 = new Number160( 50 );

    final private static Number160 content2 = new Number160( 60 );

    final private static Number160 content3 = new Number160( 70 );

    final private static Number160 content4 = new Number160( 80 );

    private static String DIR;

    @Before
    public void befor()
        throws IOException
    {
        File tmpDir = Utils.createTempDir();
        DIR = tmpDir.getPath();
    }

    @After
    public void after()
    {
        File f = new File( DIR );
        f.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File pathname )
            {
                if ( pathname.isFile() )
                    pathname.delete();
                return false;
            }
        } );
        f.delete();
    }

    @Test
    public void testPutInitial()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        store( storageM );
        store( storageD );
        storageM.close();
        storageD.close();
    }

    private void store( StorageGeneric storage )
        throws IOException
    {
        store( storage, null, false );
    }

    private void store( StorageGeneric storage, int nr )
        throws IOException
    {
        PutStatus store =
            storage.put( locationKey, domainKey, new Number160( nr ), new Data( "test1" ), null, false, false );
        Assert.assertEquals( PutStatus.OK, store );
        store = storage.put( locationKey, domainKey, new Number160( nr ), new Data( "test2" ), null, false, false );
        Assert.assertEquals( PutStatus.OK, store );
    }

    private void store( StorageGeneric storage, PublicKey publicKey, boolean protectDomain )
        throws IOException
    {
        PutStatus store =
            storage.put( locationKey, domainKey, content1, new Data( "test1" ), publicKey, false, protectDomain );
        Assert.assertEquals( PutStatus.OK, store );
        store = storage.put( locationKey, domainKey, content2, new Data( "test2" ), publicKey, false, protectDomain );
        Assert.assertEquals( PutStatus.OK, store );
    }

    @Test
    public void testGet()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testGet( storageM );
        testGet( storageD );
        storageM.close();
        storageD.close();
    }

    private void testGet( StorageGeneric storage )
        throws IOException, ClassNotFoundException
    {
        store( storage );
        Data result1 = storage.get( locationKey, domainKey, content1 );
        Assert.assertEquals( "test1", result1.getObject() );
        Data result2 = storage.get( locationKey, domainKey, content2 );
        Assert.assertEquals( "test2", result2.getObject() );
        Data result3 = storage.get( locationKey, domainKey, content3 );
        Assert.assertEquals( null, result3 );
    }

    @Test
    public void testPut()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testPut( storageM );
        testPut( storageD );
        storageM.close();
        storageD.close();
    }

    private void testPut( StorageGeneric storage )
        throws IOException
    {
        store( storage );
        PutStatus store = storage.put( locationKey, domainKey, content1, new Data( "test3" ), null, false, false );
        Assert.assertEquals( PutStatus.OK, store );
        storage.put( locationKey, domainKey, content3, new Data( "test4" ), null, false, false );
        SortedMap<Number480, Data> result = storage.subMap( locationKey, domainKey, content1, content4 );
        Assert.assertEquals( 3, result.size() );
    }

    @Test
    public void testPutIfAbsent()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testPutIfAbsent( storageM );
        testPutIfAbsent( storageD );
        storageM.close();
        storageD.close();
    }

    private void testPutIfAbsent( StorageGeneric storage )
        throws IOException
    {
        store( storage );
        PutStatus store = storage.put( locationKey, domainKey, content1, new Data( "test3" ), null, true, false );
        Assert.assertEquals( PutStatus.FAILED_NOT_ABSENT, store );
        storage.put( locationKey, domainKey, content3, new Data( "test4" ), null, true, false );
        SortedMap<Number480, Data> result1 = storage.subMap( locationKey, domainKey, content1, content4 );
        Assert.assertEquals( 3, result1.size() );
        SortedMap<Number480, Data> result2 = storage.subMap( locationKey, domainKey, content1, content3 );
        Assert.assertEquals( 2, result2.size() );
    }

    @Test
    public void testRemove()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testRemove( storageM );
        testRemove( storageD );
        storageM.close();
        storageD.close();
    }

    private void testRemove( StorageGeneric storage )
        throws IOException, ClassNotFoundException
    {
        store( storage );
        Data result1 = storage.remove( locationKey, domainKey, content1 );
        Assert.assertEquals( "test1", result1.getObject() );
        SortedMap<Number480, Data> result2 = storage.subMap( locationKey, domainKey, content1, content4 );
        Assert.assertEquals( 1, result2.size() );
        store( storage );
        SortedMap<Number480, Data> result3 = storage.remove( locationKey, domainKey, content1, content4, null );
        Assert.assertEquals( 2, result3.size() );
        SortedMap<Number480, Data> result4 = storage.subMap( locationKey, domainKey, content1, content4 );
        Assert.assertEquals( 0, result4.size() );
    }

    @Test
    public void testTTL1()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testTTL1( storageM );
        testTTL1( storageD );
        storageM.close();
        storageD.close();
    }

    private void testTTL1( StorageGeneric storage )
        throws Exception
    {
        Data data = new Data( "string" );
        data.setTTLSeconds( 0 );
        storage.put( locationKey, domainKey, content1, data, null, false, false );
        Thread.sleep( 2000 );
        Data tmp = storage.get( locationKey, domainKey, content1 );
        Assert.assertEquals( true, tmp != null );
    }

    @Test
    public void testTTL2()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testTTL2( storageM );
        testTTL2( storageD );
        storageM.close();
        storageD.close();
    }

    private void testTTL2( StorageGeneric storage )
        throws Exception
    {
        Data data = new Data( "string" );
        data.setTTLSeconds( 1 );
        storage.put( locationKey, domainKey, content1, data, null, false, false );
        Thread.sleep( 2000 );
        storage.checkTimeout();
        Data tmp = storage.get( locationKey, domainKey, content1 );
        Assert.assertEquals( true, tmp == null );
    }

    @Test
    public void testResponsibility()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testResponsibility( storageM );
        testResponsibility( storageD );
        storageM.close();
        storageD.close();
    }

    private void testResponsibility( StorageGeneric storage )
        throws Exception
    {
        storage.updateResponsibilities( content1, locationKey );
        storage.updateResponsibilities( content2, locationKey );
        Assert.assertEquals( locationKey, storage.findPeerIDForResponsibleContent( content1 ) );
        Assert.assertEquals( 2, storage.findContentForResponsiblePeerID( locationKey ).size() );
        storage.updateResponsibilities( content1, domainKey );
        storage.updateResponsibilities( content2, locationKey );
        Assert.assertEquals( domainKey, storage.findPeerIDForResponsibleContent( content1 ) );
    }

    @Test
    public void testPublicKeyDomain()
        throws Exception
    {
        StorageGeneric storageM = new StorageMemory();
        StorageGeneric storageD = new StorageDisk( DIR );
        testPublicKeyDomain( storageM );
        testPublicKeyDomain( storageD );
        storageM.close();
        storageD.close();
    }

    private void testPublicKeyDomain( StorageGeneric storage )
        throws Exception
    {
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        store( storage, pair1.getPublic(), true );
        PutStatus result1 =
            storage.put( locationKey, domainKey, content3, new Data( "test4" ), pair1.getPublic(), false, false );
        Assert.assertEquals( PutStatus.OK, result1 );
        PutStatus result3 =
            storage.put( locationKey, domainKey, content3, new Data( "test6" ), pair1.getPublic(), false, true );
        Assert.assertEquals( PutStatus.OK, result3 );
        // domain is protected by pair1
        PutStatus result2 =
            storage.put( locationKey, domainKey, content3, new Data( "test5" ), pair2.getPublic(), false, true );
        Assert.assertEquals( PutStatus.FAILED, result2 );
    }

    @Test
    public void testLock1()
    {
        KeyLock<Number160> lock = new KeyLock<Number160>();
        Lock tmp = lock.lock( Number160.createHash( "test" ) );
        Assert.assertEquals( 1, lock.cacheSize() );
        lock.unlock( Number160.createHash( "test" ), tmp );
        Assert.assertEquals( 0, lock.cacheSize() );
        lock.unlock( Number160.createHash( "test" ), tmp );
    }

    @Test
    public void testLock2()
    {
        KeyLock<Number160> lock = new KeyLock<Number160>();
        Lock tmp1 = lock.lock( Number160.createHash( "test1" ) );
        Lock tmp2 = lock.lock( Number160.createHash( "test2" ) );
        Assert.assertEquals( 2, lock.cacheSize() );
        lock.unlock( Number160.createHash( "test1" ), tmp1 );
        lock.unlock( Number160.createHash( "test2" ), tmp2 );
        Assert.assertEquals( 0, lock.cacheSize() );
    }

    @Test
    public void testLockConcurrent()
        throws InterruptedException
    {
        final KeyLock<Number160> lock = new KeyLock<Number160>();
        for ( int i = 0; i < 100; i++ )
        {
            final int ii = i;
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    Lock tmp1 = lock.lock( Number160.createHash( "test1" ) );
                    Lock tmp2 = lock.lock( Number160.createHash( "test2" ) );
                    lock.unlock( Number160.createHash( "test1" ), tmp1 );
                    lock.unlock( Number160.createHash( "test2" ), tmp2 );
                    System.err.print( "a" + ii + " " );
                }
            } ).start();
        }
        for ( int i = 0; i < 100; i++ )
        {
            final int ii = i;
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    Lock tmp1 = lock.lock( Number160.createHash( "test3" ) );
                    Lock tmp2 = lock.lock( Number160.createHash( "test4" ) );
                    lock.unlock( Number160.createHash( "test3" ), tmp1 );
                    lock.unlock( Number160.createHash( "test4" ), tmp2 );
                    System.err.print( "b" + ii + " " );
                }
            } ).start();
        }
        Thread.sleep( 500 );
        Assert.assertEquals( 0, lock.cacheSize() );
        Lock tmp1 = lock.lock( Number160.createHash( "test1" ) );
        Lock tmp2 = lock.lock( Number160.createHash( "test2" ) );
        Assert.assertEquals( 2, lock.cacheSize() );
        lock.unlock( Number160.createHash( "test1" ), tmp1 );
        lock.unlock( Number160.createHash( "test1" ), tmp2 );
        Assert.assertEquals( 1, lock.cacheSize() );
    }

    @Test
    public void testConcurrency()
        throws InterruptedException, IOException
    {
        final StorageGeneric sM = new StorageMemory();
        final StorageGeneric sD = new StorageDisk( DIR );
        store( sM );
        store( sD );
        final AtomicInteger counter = new AtomicInteger();
        for ( int i = 0; i < 100; i++ )
        {
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Data result1 = sM.get( locationKey, domainKey, content1 );
                        Assert.assertEquals( "test1", result1.getObject() );
                        Data result2 = sM.get( locationKey, domainKey, content2 );
                        Assert.assertEquals( "test2", result2.getObject() );
                        Data result3 = sM.get( locationKey, domainKey, content3 );
                        Assert.assertEquals( null, result3 );

                        result1 = sD.get( locationKey, domainKey, content1 );
                        Assert.assertEquals( "test1", result1.getObject() );
                        result2 = sD.get( locationKey, domainKey, content2 );
                        Assert.assertEquals( "test2", result2.getObject() );
                        result3 = sD.get( locationKey, domainKey, content3 );
                        Assert.assertEquals( null, result3 );

                        store( sM, 1 );
                        store( sD, 1 );
                    }
                    catch ( Throwable t )
                    {
                        t.printStackTrace();
                        counter.incrementAndGet();
                    }
                }
            } ).start();
        }
        Thread.sleep( 500 );
        Assert.assertEquals( 0, counter.get() );
        sM.close();
        sD.close();
    }
}