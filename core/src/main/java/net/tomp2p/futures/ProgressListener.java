package net.tomp2p.futures;

import net.tomp2p.message.Message2;

public interface ProgressListener {

    void progress(Message2 interMediateMessage);

}
