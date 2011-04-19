package net.tomp2p.storage;
import java.io.File;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.tomp2p.message.DataInput;
import net.tomp2p.message.DataOutput;
import net.tomp2p.message.DataOutputFactory;
import net.tomp2p.message.MessageCodec;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.rpc.DigestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleTupleKeyCreator;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class StorageDisk extends Storage
{
	final private static Logger logger = LoggerFactory.getLogger(StorageDisk.class);
	private final Environment env;
	private final StoredClassCatalog javaCatalog;
	private final Database dataDb;
	private final Database protectionDb;
	private final Database timeoutDb;
	private final Database responsibilityDb;
	private final SecondaryDatabase timeoutByLong;
	private final SecondaryDatabase responsibilityByNumber160;
	private final SecondaryDatabase dataByReplication;
	//
	private final StoredSortedMap<Number480, Data> dataMap;
	private final StoredMap<Number480, Data> dataMapReplication;
	private final StoredMap<Number320, PublicKey> protectedMap;
	private final StoredMap<Number480, Number480Long> timeoutMap;
	private final StoredSortedMap<Long, Number480Long> timeoutMapRev;
	private final StoredMap<Number160, Number160Number160> responsibilityMap;
	private final StoredMap<Number160, Number160Number160> responsibilityMapRev;

	public StorageDisk(String homeDirectory) throws Exception
	{
		new File(homeDirectory).mkdirs();
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(true);
		envConfig.setAllowCreate(true);
		env = new Environment(new File(homeDirectory), envConfig);
		// create cataloge
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);
		dbConfig.setAllowCreate(true);
		Database catalogDb = env.openDatabase(null, "java_class_catalog", dbConfig);
		javaCatalog = new StoredClassCatalog(catalogDb);
		// open DB and make binding to map
		dataDb = env.openDatabase(null, "data_store", dbConfig);
		protectionDb = env.openDatabase(null, "protection_store", dbConfig);
		timeoutDb = env.openDatabase(null, "timeout_store", dbConfig);
		responsibilityDb = env.openDatabase(null, "responsibilyt_store", dbConfig);
		// bindings
		TupleBinding<Number480> number480Binding = new Number480Binding();
		TupleBinding<Number320> number320Binding = new Number320Binding();
		TupleBinding<PublicKey> publicKeyValueBinding = new PublicKeyValueBinding();
		TupleBinding<Number480Long> number480longBinding = new Number480LongBinding(
				number480Binding);
		TupleBinding<Long> longBinding = new LongBinding();
		TupleBinding<Number160> number160Binding = new Number160Binding();
		TupleBinding<Data> dataValueBinding = new DataBinding();
		TupleBinding<Number160Number160> number160number160Binding = new Number160Number160Binding(
				number160Binding);
		// secondary DB for timeout
		SecondaryConfig secConfig1 = new SecondaryConfig();
		secConfig1.setTransactional(true);
		secConfig1.setAllowCreate(true);
		secConfig1.setSortedDuplicates(true);
		secConfig1.setKeyCreator(new TimeoutByLongKeyCreator(number480longBinding));
		timeoutByLong = env
				.openSecondaryDatabase(null, "timeout_store_long", timeoutDb, secConfig1);
		// secondary DB for responsibility
		SecondaryConfig secConfig2 = new SecondaryConfig();
		secConfig2.setTransactional(true);
		secConfig2.setAllowCreate(true);
		secConfig2.setSortedDuplicates(true);
		secConfig2.setKeyCreator(new ResponsibilityByNumber160KeyCreator(number160number160Binding,
				number160Binding));
		responsibilityByNumber160 = env.openSecondaryDatabase(null,
				"responsibilyt_store_number160", responsibilityDb, secConfig2);
		// secondary DB for data
		SecondaryConfig secConfig3 = new SecondaryConfig();
		secConfig3.setTransactional(true);
		secConfig3.setAllowCreate(true);
		secConfig3.setSortedDuplicates(true);
		secConfig3.setKeyCreator(new DataByNumber480KeyCreator(dataValueBinding, number480Binding));
		dataByReplication = env.openSecondaryDatabase(null, "data_store_boolean", dataDb,
				secConfig3);
		// create the views
		dataMap = new StoredSortedMap<Number480, Data>(dataDb, number480Binding, dataValueBinding,
				true);
		dataMapReplication = new StoredMap<Number480, Data>(dataByReplication, number480Binding,
				dataValueBinding, true);
		protectedMap = new StoredMap<Number320, PublicKey>(protectionDb, number320Binding,
				publicKeyValueBinding, true);
		timeoutMap = new StoredMap<Number480, Number480Long>(timeoutDb, number480Binding,
				number480longBinding, true);
		timeoutMapRev = new StoredSortedMap<Long, Number480Long>(timeoutByLong, longBinding,
				number480longBinding, true);
		responsibilityMap = new StoredMap<Number160, Number160Number160>(responsibilityDb,
				number160Binding, number160number160Binding, true);
		responsibilityMapRev = new StoredSortedMap<Number160, Number160Number160>(
				responsibilityByNumber160, number160Binding, number160number160Binding, true);
	}

	@Override
	public void close()
	{
		dataByReplication.close();
		dataDb.close();
		protectionDb.close();
		timeoutByLong.close();
		timeoutDb.close();
		responsibilityByNumber160.close();
		responsibilityDb.close();
		javaCatalog.close();
		env.close();
	}

	@Override
	public boolean put(Number480 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
			boolean domainProtection)
	{
		checkTimeout();
		Transaction txn = env.beginTransaction(null, null);
		try
		{
			if (!securityDomainCheck(key, publicKey, domainProtection))
			{
				txn.abort();
				return false;
			}
			boolean contains = dataMap.containsKey(key);
			if (putIfAbsent && contains)
			{
				txn.abort();
				return false;
			}
			if (contains)
			{
				Data oldData = dataMap.get(key);
				boolean protectEntry = newData.isProtectedEntry();
				if (!canUpdateEntry(key, oldData, newData, protectEntry))
				{
					txn.abort();
					return false;
				}
			}
			dataMap.put(key, newData);
			timeoutMap.put(key, new Number480Long(key, newData.getExpirationMillis()));
			txn.commit();
			return true;
		}
		catch (Exception e)
		{
			logger.error(e.toString());
			e.printStackTrace();
			if (txn != null)
				txn.abort();
			return false;
		}
	}

	private boolean securityDomainCheck(Number480 key, PublicKey publicKey, boolean domainProtection)
	{
		if (domainProtection)
		{
			Number320 partKey = new Number320(key.getLocationKey(), key.getDomainKey());
			if (canProtectDomain(partKey, publicKey)
					&& !isDomainProtectedByOthers(partKey, publicKey))
				return protectDomain(partKey, publicKey);
			else
				return false;
		}
		return true;
	}

	private boolean isDomainProtectedByOthers(Number320 partKey, PublicKey publicKey)
	{
		PublicKey other = protectedMap.get(partKey);
		if (other == null)
			return false;
		return !publicKey.equals(other);
	}

	private boolean protectDomain(Number320 partKey, PublicKey publicKey)
	{
		if (!protectedMap.containsKey(partKey))
		{
			if (getProtectionEntryInDomain() == ProtectionEntryInDomain.ENTRY_REMOVE_IF_DOMAIN_CLAIMED)
				remove(partKey.min(), partKey.max(), publicKey);
			protectedMap.put(partKey, publicKey);
			return true;
		}
		else
			// or else check if already protected
			return protectedMap.get(partKey).equals(publicKey);
	}

	@Override
	public Data get(Number480 key)
	{
		checkTimeout();
		return dataMap.get(key);
	}

	@Override
	public SortedMap<Number480, Data> get(Number480 fromKey, Number480 toKey)
	{
		checkTimeout();
		if (fromKey == null && toKey == null)
			return null;
		else if (toKey == null)
			return dataMap.tailMap(fromKey);
		else if (fromKey == null)
			return dataMap.headMap(toKey);
		else
			return dataMap.subMap(fromKey, toKey);
	}

	@Override
	public Data remove(Number480 key, PublicKey publicKey)
	{
		checkTimeout();
		return remove(key, publicKey, false);
	}

	private Data remove(Number480 key, PublicKey publicKey, boolean force)
	{
		Transaction txn = env.beginTransaction(null, null);
		try
		{
			if (!force
					&& isDomainProtectedByOthers(new Number320(key.getLocationKey(), key
							.getDomainKey()), publicKey))
			{
				txn.abort();
				return null;
			}
			Data data = dataMap.get(key);
			if (force || data.getDataPublicKey() == null
					|| data.getDataPublicKey().equals(publicKey))
			{
				timeoutMap.remove(key);
				responsibilityMap.remove(key.getLocationKey());
				Data removedData = dataMap.remove(key);
				txn.commit();
				return removedData;
			}
			else
			{
				txn.abort();
				return null;
			}
		}
		catch (Exception e)
		{
			logger.error(e.toString());
			e.printStackTrace();
			if (txn != null)
				txn.abort();
			return null;
		}
	}

	@Override
	public SortedMap<Number480, Data> remove(Number480 fromKey, Number480 toKey, PublicKey publicKey)
	{
		checkTimeout();
		Transaction txn = env.beginTransaction(null, null);
		try
		{
			// we remove only if locationkey and domain key are the same
			if (!fromKey.getLocationKey().equals(toKey.getLocationKey())
					|| !fromKey.getDomainKey().equals(toKey.getDomainKey()))
			{
				txn.abort();
				return null;
			}
			if (isDomainProtectedByOthers(new Number320(fromKey.getLocationKey(), fromKey
					.getDomainKey()), publicKey))
			{
				txn.abort();
				return null;
			}
			SortedMap<Number480, Data> tmp = dataMap.subMap(fromKey, toKey);
			Collection<Number480> keys = new ArrayList<Number480>(tmp.keySet());
			SortedMap<Number480, Data> result = new TreeMap<Number480, Data>();
			for (Number480 key : keys)
			{
				Data data = dataMap.get(key);
				if (data.getDataPublicKey() == null || data.getDataPublicKey().equals(publicKey))
				{
					timeoutMap.remove(key);
					responsibilityMap.remove(key.getLocationKey());
					result.put(key, dataMap.remove(key));
				}
			}
			txn.commit();
			return result;
		}
		catch (Exception e)
		{
			logger.error(e.toString());
			e.printStackTrace();
			if (txn != null)
				txn.abort();
			return null;
		}
	}

	@Override
	public boolean contains(Number480 key)
	{
		checkTimeout();
		return dataMap.containsKey(key);
	}
	
	@Override
	public DigestInfo digest(Number320 key)
	{
		checkTimeout();
		SortedMap<Number480, Data> tmp = get(key);
		Number160 hash = Number160.ZERO;
		for (Number480 key2 : tmp.keySet())
			hash = hash.xor(key2.getContentKey());
		return new DigestInfo(hash, tmp.size());
	}

	@Override
	public DigestInfo digest(Number320 key, Collection<Number160> contentKeys)
	{
		if (contentKeys == null)
			return digest(key);
		checkTimeout();
		SortedMap<Number480, Data> tmp = get(key);
		Number160 hash = Number160.ZERO;
		int size = 0;
		for (Number480 key2 : tmp.keySet())
		{
			if(contentKeys.contains(key2.getContentKey()))
			{
				hash = hash.xor(key2.getContentKey());
				size++;
			}
		}
		return new DigestInfo(hash, size);
	}

	@Override
	public DigestInfo digest(Number320 key, Number160 fromKey, Number160 toKey)
	{
		if (fromKey == null)
			fromKey = Number160.ZERO;
		if (toKey == null)
			toKey = Number160.MAX_VALUE;
		checkTimeout();
		SortedMap<Number480, Data> tmp = get(key);
		Number160 hash = Number160.ZERO;
		int size = 0;
		for (Number480 key2 : tmp.keySet())
		{
			if(fromKey.compareTo(key2.getContentKey())<1 && toKey.compareTo(key2.getContentKey())>1)
			{
				hash = hash.xor(key2.getContentKey());
				size++;
			}
		}
		return new DigestInfo(hash, size);
	}

	@Override
	public void iterateAndRun(Number160 locationKey, StorageRunner runner)
	{
		Number480 min = new Number480(locationKey, Number160.ZERO, Number160.ZERO);
		Number480 max = new Number480(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
		checkTimeout();
		for (Map.Entry<Number480, Data> entry : dataMap.subMap(min, max).entrySet())
		{
			runner.call(entry.getKey().getLocationKey(), entry.getKey().getDomainKey(), entry
					.getKey().getContentKey(), entry.getValue());
		}
	}

	@Override
	public Collection<Number160> findResponsibleData(Number160 peerID)
	{
		Collection<Number160> result = new ArrayList<Number160>();
		for (Number160Number160 tmp : responsibilityMapRev.duplicates(peerID))
			result.add(tmp.getNumberKey());
		return result;
	}

	@Override
	public Number160 findResponsiblePeerID(Number160 locationKey)
	{
		Number160Number160 tmp = responsibilityMap.get(locationKey);
		if (tmp == null)
			return null;
		return tmp.getNumberPeerID();
	}

	@Override
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerID)
	{
		Number160Number160 old = responsibilityMap.put(locationKey, new Number160Number160(
				locationKey, peerID));
		if (old == null)
			return true;
		return !old.numberPeerID.equals(peerID);
	}

	@Override
	public Collection<Number480> storedDirectReplication()
	{
		return dataMapReplication.keySet();
	}

	// TODO: make check timeout time based in a thread, but for now its ok.
	private Collection<Number480> checkTimeout()
	{
		Transaction txn = env.beginTransaction(null, null);
		try
		{
			List<Number480> toRemove = new ArrayList<Number480>();
			for (Map.Entry<Long, Number480Long> entry : timeoutMapRev.subMap(0L,
					System.currentTimeMillis()).entrySet())
			{
				toRemove.add(entry.getValue().getNumber480());
			}
			if (toRemove.size() > 0)
			{
				for (Number480 key : toRemove)
				{
					logger.debug("Remove key " + key + " due to expiration");
					remove(key, null, true);
				}
			}
			txn.commit();
			return toRemove;
		}
		catch (Exception e)
		{
			logger.error(e.toString());
			e.printStackTrace();
			if (txn != null)
				txn.abort();
			return null;
		}
	}
	private static class Number160Number160Binding extends TupleBinding<Number160Number160>
	{
		private final TupleBinding<Number160> tupleBinding160;

		private Number160Number160Binding(TupleBinding<Number160> tupleBinding160)
		{
			this.tupleBinding160 = tupleBinding160;
		}

		@Override
		public Number160Number160 entryToObject(TupleInput input)
		{
			Number160 numberKey = tupleBinding160.entryToObject(input);
			Number160 numberPeerID = tupleBinding160.entryToObject(input);
			return new Number160Number160(numberKey, numberPeerID);
		}

		@Override
		public void objectToEntry(Number160Number160 object, TupleOutput output)
		{
			tupleBinding160.objectToEntry(object.getNumberKey(), output);
			tupleBinding160.objectToEntry(object.getNumberPeerID(), output);
		}
	}
	private static class Number480LongBinding extends TupleBinding<Number480Long>
	{
		private final TupleBinding<Number480> tupleBinding;

		private Number480LongBinding(TupleBinding<Number480> tupleBinding)
		{
			this.tupleBinding = tupleBinding;
		}

		@Override
		public Number480Long entryToObject(TupleInput input)
		{
			long longValue = input.readLong();
			Number480 number480 = tupleBinding.entryToObject(input);
			return new Number480Long(number480, longValue);
		}

		@Override
		public void objectToEntry(Number480Long object, TupleOutput output)
		{
			output.writeLong(object.getLongValue());
			tupleBinding.objectToEntry(object.getNumber480(), output);
		}
	}
	private static class Number480Binding extends TupleBinding<Number480>
	{
		@Override
		public Number480 entryToObject(TupleInput input)
		{
			byte[] first = new byte[Number160.BYTE_ARRAY_SIZE];
			byte[] second = new byte[Number160.BYTE_ARRAY_SIZE];
			byte[] third = new byte[Number160.BYTE_ARRAY_SIZE];
			input.read(first);
			input.read(second);
			input.read(third);
			return new Number480(new Number160(first), new Number160(second), new Number160(third));
		}

		@Override
		public void objectToEntry(Number480 object, TupleOutput output)
		{
			output.write(object.getLocationKey().toByteArray());
			output.write(object.getDomainKey().toByteArray());
			output.write(object.getContentKey().toByteArray());
		}
	}
	private static class Number160Binding extends TupleBinding<Number160>
	{
		@Override
		public Number160 entryToObject(TupleInput input)
		{
			byte[] first = new byte[Number160.BYTE_ARRAY_SIZE];
			input.read(first);
			return new Number160(first);
		}

		@Override
		public void objectToEntry(Number160 object, TupleOutput output)
		{
			output.write(object.toByteArray());
		}
	}
	private static class Number320Binding extends TupleBinding<Number320>
	{
		@Override
		public Number320 entryToObject(TupleInput input)
		{
			byte[] first = new byte[Number160.BYTE_ARRAY_SIZE];
			byte[] second = new byte[Number160.BYTE_ARRAY_SIZE];
			byte[] third = new byte[Number160.BYTE_ARRAY_SIZE];
			input.read(first);
			input.read(second);
			input.read(third);
			return new Number320(new Number160(first), new Number160(second));
		}

		@Override
		public void objectToEntry(Number320 object, TupleOutput output)
		{
			output.write(object.getLocationKey().toByteArray());
			output.write(object.getDomainKey().toByteArray());
		}
	}
	private static class LongBinding extends TupleBinding<Long>
	{
		@Override
		public Long entryToObject(TupleInput input)
		{
			return input.readLong();
		}

		@Override
		public void objectToEntry(Long object, TupleOutput output)
		{
			output.writeLong(object.longValue());
		}
	}
	private static class DataBinding extends TupleBinding<Data>
	{
		@Override
		public Data entryToObject(TupleInput input)
		{
			try
			{
				Data data = MessageCodec.decodeData(new TupleDecoder(input), null);
				// in addition to, we need to decode if we have a direct
				// replication. This is not done in MessageCodec becaues this
				// information does not go over the wire.
				int flag = input.readByte();
				boolean isDirectReplication = (flag & 0x1) > 0;
				data.setDirectReplication(isDirectReplication);
				return data;
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		@Override
		public void objectToEntry(Data object, TupleOutput output)
		{
			TupleFactory factory = new TupleFactory(output);
			MessageCodec.encodeData(new ArrayList<DataOutput>(), factory, null, object);
			// in addition, we need to encode if we have a direct replication.
			// This is not done in MessageCodec becaues this information does
			// not go over the wire.
			int flag = 0;
			if (object.isDirectReplication())
				flag |= 0x1;
			output.writeByte(flag);
		}
	}
	private static class PublicKeyValueBinding extends TupleBinding<PublicKey>
	{
		@Override
		public PublicKey entryToObject(TupleInput input)
		{
			int len = input.readShort() & 0xffff;
			byte[] data = new byte[len];
			input.read(data);
			try
			{
				return MessageCodec.decodePublicKey(new TupleDecoder(input), data);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		@Override
		public void objectToEntry(PublicKey object, TupleOutput output)
		{
			byte[] enc = object.getEncoded();
			output.writeShort(enc.length);
			output.write(enc);
		}
	}
	private static class DataByNumber480KeyCreator extends TupleTupleKeyCreator<Number480>
	{
		private final TupleBinding<Data> bindingData;
		private final TupleBinding<Number480> bindingNumber480;

		public DataByNumber480KeyCreator(TupleBinding<Data> bindingData,
				TupleBinding<Number480> bindingNumber480)
		{
			this.bindingData = bindingData;
			this.bindingNumber480 = bindingNumber480;
		}

		@Override
		public boolean createSecondaryKey(TupleInput primaryKeyInput, TupleInput dataInput,
				TupleOutput indexKeyOutput)
		{
			Data data = bindingData.entryToObject(dataInput);
			Number480 key = bindingNumber480.entryToObject(primaryKeyInput);
			if (data.isDirectReplication())
			{
				bindingNumber480.objectToEntry(key, indexKeyOutput);
				return true;
			}
			return false;
		}
	}
	private static class TimeoutByLongKeyCreator extends TupleTupleKeyCreator<Long>
	{
		private final TupleBinding<Number480Long> binding;

		public TimeoutByLongKeyCreator(TupleBinding<Number480Long> binding)
		{
			this.binding = binding;
		}

		@Override
		public boolean createSecondaryKey(TupleInput primaryKeyInput, TupleInput dataInput,
				TupleOutput indexKeyOutput)
		{
			Number480Long data = binding.entryToObject(dataInput);
			indexKeyOutput.writeLong(data.getLongValue());
			return true;
		}
	}
	private static class ResponsibilityByNumber160KeyCreator extends
			TupleTupleKeyCreator<Number160>
	{
		private final TupleBinding<Number160Number160> bindingInput;
		private final TupleBinding<Number160> bindingOutput;

		public ResponsibilityByNumber160KeyCreator(TupleBinding<Number160Number160> bindingInput,
				TupleBinding<Number160> bindingOutput)
		{
			this.bindingInput = bindingInput;
			this.bindingOutput = bindingOutput;
		}

		@Override
		public boolean createSecondaryKey(TupleInput primaryKeyInput, TupleInput dataInput,
				TupleOutput indexKeyOutput)
		{
			Number160Number160 data = bindingInput.entryToObject(dataInput);
			bindingOutput.objectToEntry(data.getNumberPeerID(), indexKeyOutput);
			return true;
		}
	}
	private static class Number480Long
	{
		private final Number480 number480;
		private final Long longValue;

		private Number480Long(Number480 number480, Long longValue)
		{
			this.number480 = number480;
			this.longValue = longValue;
		}

		public Number480 getNumber480()
		{
			return number480;
		}

		public Long getLongValue()
		{
			return longValue;
		}
	}
	private static class Number160Number160
	{
		private final Number160 numberKey;
		private final Number160 numberPeerID;

		private Number160Number160(Number160 numberKey, Number160 numberPeerID)
		{
			this.numberKey = numberKey;
			this.numberPeerID = numberPeerID;
		}

		public Number160 getNumberKey()
		{
			return numberKey;
		}

		public Number160 getNumberPeerID()
		{
			return numberPeerID;
		}
	}
	private static class TupleFactory implements DataOutputFactory
	{
		final private TupleEncoder encoder;

		private TupleFactory(TupleOutput output)
		{
			this.encoder = new TupleEncoder(output);
		}

		@Override
		public DataOutput create(int count)
		{
			return encoder;
		}

		@Override
		public DataOutput create(byte[] data, int offset, int length)
		{
			encoder.writeBytes(data, offset, length);
			return encoder;
		}

		@Override
		public DataOutput create(byte[] data)
		{
			encoder.writeBytes(data);
			return encoder;
		}
	}
	private static class TupleEncoder implements DataOutput
	{
		final private TupleOutput output;

		private TupleEncoder(TupleOutput output)
		{
			this.output = output;
		}

		public void writeBytes(byte[] data)
		{
			output.write(data);
		}

		public void writeBytes(byte[] data, int offset, int length)
		{
			output.write(data, offset, length);
		}

		@Override
		public void writeByte(int intVal)
		{
			output.writeUnsignedByte(intVal);
		}

		@Override
		public void writeInt(int intVal)
		{
			output.writeInt(intVal);
		}

		@Override
		public void writeShort(int intVal)
		{
			output.writeUnsignedShort(intVal);
		}
	}
	private static class TupleDecoder implements DataInput
	{
		final private TupleInput input;

		private TupleDecoder(TupleInput input)
		{
			this.input = input;
		}

		@Override
		public byte[] array()
		{
			return input.getBufferBytes();
		}

		@Override
		public int arrayOffset()
		{
			return input.getBufferOffset();
		}

		@Override
		public void readBytes(byte[] buf)
		{
			input.read(buf);
		}

		@Override
		public int readInt()
		{
			return input.readInt();
		}

		@Override
		public int readUnsignedByte()
		{
			return input.readUnsignedByte() & 0xff;
		}

		@Override
		public int getUnsignedByte()
		{
			return input.getBufferBytes()[input.getBufferOffset() & 0xff];
		}

		@Override
		public int readUnsignedShort()
		{
			return input.readUnsignedShort() & 0xffff;
		}

		@Override
		public int readerIndex()
		{
			return 0;
		}

		@Override
		public void skipBytes(int size)
		{
			input.skipFast(size);
		}
	}
}
