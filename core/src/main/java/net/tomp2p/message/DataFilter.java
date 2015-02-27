package net.tomp2p.message;

import net.tomp2p.storage.Data;

public interface DataFilter {

	Data filter(Data data, boolean isConvertMeta, boolean isReply);

}
