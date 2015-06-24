package net.tomp2p.holep.manual;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Command implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static Map<Object, Object> global = new HashMap<Object, Object>();
	
	public void put(Object key, Object value) {
		synchronized (global) {
			global.put(key, value);	
		}
		
	}
	
	public Object get(Object key) {
		synchronized (global) {
			return global.get(key);
		}
	}
	
	public abstract Serializable execute() throws Exception;
}
