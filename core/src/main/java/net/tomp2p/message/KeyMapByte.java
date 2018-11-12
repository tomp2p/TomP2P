package net.tomp2p.message;

import java.util.Map;

import net.tomp2p.utils.Utils;

public class KeyMapByte {

    private final Map<Object, Byte> keysMap;
    public KeyMapByte(Map<Object, Byte> keysMap) {
        this.keysMap = keysMap;
    }
    
    public Map<Object, Byte> keysMap() {
        return keysMap;
    }

    public int size() {
        return keysMap.size();
    }

    public void put(Object key, Byte value) {
        keysMap.put(key, value);
    }
    
    @Override
    public int hashCode() {
        return keysMap.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyMapByte)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final KeyMapByte k = (KeyMapByte) obj;
        final boolean test1 = Utils.isSameSets(k.keysMap.keySet(), keysMap.keySet());
        final boolean test2 = Utils.isSameSets(k.keysMap.values(), keysMap.values());
        return test1 && test2;
    }
}
