package net.tomp2p.futures;

import net.tomp2p.message.Message;

public interface ProgressListener {

    void progress(Message interMediateMessage);

}
