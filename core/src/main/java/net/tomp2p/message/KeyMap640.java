package net.tomp2p.message;

import java.util.NavigableMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.utils.Utils;

public class KeyMap640 {

    private final NavigableMap<Number640, Number160> keysMap;
    public KeyMap640(NavigableMap<Number640, Number160> keysMap) {
        this.keysMap = keysMap;
    }
    
    public NavigableMap<Number640, Number160> keysMap() {
        return keysMap;
    }

    public int size() {
        return keysMap.size();
    }

    public void put(Number640 key, Number160 value) {
        keysMap.put(key, value);
    }
    
    @Override
    public int hashCode() {
        return keysMap.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyMap640)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final KeyMap640 k = (KeyMap640) obj;
        final boolean test1 = Utils.isSameSets(k.keysMap.keySet(), keysMap.keySet());
        final boolean test2 = Utils.isSameSets(k.keysMap.values(), keysMap.values());
        return test1 && test2;
    }
}
