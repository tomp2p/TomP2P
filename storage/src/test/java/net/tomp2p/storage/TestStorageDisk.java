package net.tomp2p.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.peers.Number160;

import org.junit.After;
import org.junit.Before;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class TestStorageDisk extends TestStorage {
	final private static Number160 locationKey = new Number160(10);
	private static File DIR;

	public Storage createStorage() throws IOException {
		DB db = DBMaker.newFileDB(new File(DIR, "tomp2p")).transactionDisable().closeOnJvmShutdown().cacheDisable().make();
		return new StorageDisk(db, locationKey, DIR, new DSASignatureFactory());
	}

	@Before
	public void befor() throws IOException {
		DIR =  Files.createTempDirectory("tomp2p").toFile();
	}

	@After
	public void after() {
		DIR.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.isFile())
					pathname.delete();
				return false;
			}
		});
		DIR.delete();
	}
}