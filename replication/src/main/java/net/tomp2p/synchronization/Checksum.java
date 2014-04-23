/*
 * Copyright 2013 Maxat Pernebayev, Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.synchronization;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Class that holds the checksum, a weak rolling checksum and a strong checksum.
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
public class Checksum implements Serializable {

    private static final long serialVersionUID = -5313140351556914101L;
    private final int weakChecksum;
    private final byte[] strongChecksum;
    
    public Checksum(int weakChecksum, byte[] strongChecksum) {
    	this.weakChecksum = weakChecksum;
    	this.strongChecksum = strongChecksum;
    }

    public int weakChecksum() {
        return weakChecksum;
    }

    public byte[] strongChecksum() {
        return strongChecksum;
    }
    
    public int length() {
    	return 4 + strongChecksum.length; 
    }
    
    @Override
    public int hashCode() {
        return weakChecksum ^ (strongChecksum == null ? 0 : Arrays.hashCode(strongChecksum));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Checksum)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        Checksum c = (Checksum) obj;
        return weakChecksum == c.weakChecksum && Arrays.equals(strongChecksum, c.strongChecksum);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("wcs:");
        sb.append(weakChecksum).append(",scs:").append(Arrays.toString(strongChecksum));
        return sb.toString();
    }
}
