package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureLaterJoin;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.storage.StorageGeneric;

import net.tomp2p.task.Worker;

public class ExampleMapReduce
{
	final private static Random rnd = new Random(42L);
	
	final public static String text1 = "to be or not to be";
	final public static String text2 = "to do is to be";
	final public static String text3 = "to be is to do";
	
	public static void main(String[] args) throws Exception
	{
	}
	
	private static class Pair
	{
		private final String string;
		private final Integer integer;
		public Pair(String string, Integer integer)
		{
			this.string = string;
			this.integer = integer;
		}
		public Integer getInteger()
		{
			return integer;
		}
		public String getString()
		{
			return string;
		}
	}
}
