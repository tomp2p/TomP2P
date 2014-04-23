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

import net.tomp2p.storage.DataBuffer;

/**
 * Class that holds the instructions what to do with the differences.
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
public class Instruction implements Serializable {

    private static final long serialVersionUID = 112641683009283845L;
    private final int reference;
    private final DataBuffer literal;
    
    public Instruction(int reference) {
        this.reference = reference;
        this.literal = null;
    }

    public Instruction (DataBuffer literal) {
    	this.reference = -1;
        this.literal = literal;
    }

    public int reference() {
        return reference;
    }

    public DataBuffer literal() {
        return literal;
    }
    
    public int length() {
        return literal == null ? 0: literal.length();
    }
    
    @Override
    public int hashCode() {
        return reference ^ (literal == null ? 0 : literal.hashCode());
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
        return literal.equals(i.literal);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("inst");
        if (literal != null) {
        	sb.append("->l:");
        	sb.append(literal.length());
        } else {
        	sb.append("->r:");
        	sb.append(reference);
        }
        return sb.toString();
    }
}
