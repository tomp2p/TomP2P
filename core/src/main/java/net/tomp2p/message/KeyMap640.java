package net.tomp2p.message;

import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.utils.Utils;

public class KeyMap640 {

    private final Map<Number640, Number160> keysMap;
    public KeyMap640(Map<Number640, Number160> keysMap) {
        this.keysMap = keysMap;
    }
    
    public Map<Number640, Number160> keysMap() {
        return keysMap;
    }

    public int size() {
        return keysMap.size();
    }

    public void put(Number640 key, Number160 value) {
        keysMap.put(key, value);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyMap640)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        KeyMap640 k = (KeyMap640) obj;
        boolean test1 = Utils.isSameSets(k.keysMap.keySet(), keysMap.keySet());
        boolean test2 = Utils.isSameSets(k.keysMap.values(), keysMap.values());
        return test1 && test2;
    }
}
