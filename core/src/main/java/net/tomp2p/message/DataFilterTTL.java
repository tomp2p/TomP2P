package net.tomp2p.message;

import net.tomp2p.storage.Data;

public class DataFilterTTL implements DataFilter {

	@Override
	public Data filter(Data data, boolean isConvertMeta, boolean isReply) {
		final Data copyData;
		if(isConvertMeta) {
			copyData = data.duplicateMeta();
			data.release();
		} else {
			copyData = data;
		}
		if(isReply) {
			int ttl = (int) ((copyData.expirationMillis() - System.currentTimeMillis()) / 1000);
			copyData.ttlSeconds(ttl < 0 ? 0 : ttl);
		}
		return copyData;
	}

}
