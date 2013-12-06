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

package net.tomp2p.replication;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Class that holds the instructions what to do with the differences.
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
public class Instruction implements Serializable {

    private static final long serialVersionUID = 112641683009283845L;
    private int reference = -1;
    private byte[] literal = null;

    public void setReference(int reference) {
        this.reference = reference;
    }

    public void setLiteral(byte[] literal) {
        this.literal = literal;
    }

    public int getReference() {
        return reference;
    }

    public byte[] getLiteral() {
        return literal;
    }
    
    public int literalSize() {
        if(literal == null)
            return 0;
        else
            return literal.length;
    }
    
    @Override
    public int hashCode() {
        return reference ^ (literal == null ? 0 : Arrays.hashCode(literal));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Instruction)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        Instruction i = (Instruction) obj;
        if (reference >= 0) {
            return reference == i.reference;
        }
        return Arrays.equals(literal, i.literal);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("diff->r:");
        sb.append(reference);
        if (literal != null) {
            sb.append(",l:");
            for (int i = 0; i < literal.length; i++) {
                sb.append(literal[i]).append(",");
            }
        }
        return sb.toString();
    }
}
