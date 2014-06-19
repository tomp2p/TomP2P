package net.tomp2p.rcon;

import java.util.Random;

public class RandomUtil {
	private static final RandomUtil RANDOM_UTIL = new RandomUtil();
	private static final Random RANDOM = new Random(42L);
	
	//This is a Singleton
	private RandomUtil() {
		
	}
	
	public static int getNext() {
		return RANDOM.nextInt();	
	}
}
