package net.tomp2p.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Before;

public class TestStorageDisk extends TestStorage {
	final private static Number160 locationKey = new Number160(10);
	private static File DIR;

	public Storage createStorage() throws IOException {
		return new StorageDisk(DIR, locationKey);
	}

	@Before
	public void befor() throws IOException {
		DIR = File.createTempFile("tomp2p", "");
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