/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.message;

import java.io.IOException;
import java.nio.channels.FileChannel;

import org.jboss.netty.buffer.ChannelBuffer;

public interface ProtocolChunked {
    public abstract void copyToCurrent(byte[] byteArray);

    public abstract void copyToCurrent(int size);

    public abstract void copyToCurrent(byte size);

    public abstract void copyToCurrent(long long1);

    public abstract void copyToCurrent(short short1);

    public abstract void copyToCurrent(ChannelBuffer slice);

    public abstract void copyToCurrent(byte[] array, int offset, int length);

    public abstract void transferToCurrent(FileChannel inChannel, long length) throws IOException;
}