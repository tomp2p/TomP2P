package net.tomp2p.message;

import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class KeysMap {

    private final Map<Number480, Number160> keysMap;
    public KeysMap(Map<Number480, Number160> keysMap) {
        this.keysMap = keysMap;
    }
    
    public Map<Number480, Number160> keysMap() {
        return keysMap;
    }

    public int size() {
        return keysMap.size();
    }

    public void put(Number480 key, Number160 value) {
        keysMap.put(key, value);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeysMap)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        KeysMap k = (KeysMap) obj;
        boolean test1 = Utils.isSameSets(k.keysMap.keySet(), keysMap.keySet());
        boolean test2 = Utils.isSameSets(k.keysMap.values(), keysMap.values());
        return test1 && test2;
    }
}
