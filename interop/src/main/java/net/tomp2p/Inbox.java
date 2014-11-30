package net.tomp2p;

public class Inbox {

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Argument missing.");
			System.exit(-1);
		}

		String argument = args[0];
		System.out.println(String.format("Argument: %s ", argument));
		byte[] result = null;
		
		try {
			// executed .NET test
			switch (argument) {

				// primitive type encoding / decoding
				case "TestEncodeInt":
					result = DotNetEncodeDecode.testDecodeInt(argument);
					break;
				case "TestEncodeLong":
					result = DotNetEncodeDecode.testDecodeLong(argument);
					break;
				case "TestEncodeByte":
					result = DotNetEncodeDecode.testDecodeByte(argument);
					break;
				case "TestEncodeBytes":
					result = DotNetEncodeDecode.testDecodeBytes(argument);
					break;
				case "TestDecodeInt":
					result = DotNetEncodeDecode.encodeInt();
					break;
				case "TestDecodeLong":
					result = DotNetEncodeDecode.encodeLong();
					break;
				case "TestDecodeByte":
					result = DotNetEncodeDecode.encodeByte();
					break;
				case "TestDecodeBytes":
					result = DotNetEncodeDecode.encodeBytes();
					break;
					
				// Message object encoding / decoding
				case "TestMessageDecodeEmpty":
					result = MessageEncodeDecode.encodeMessageEmpty();
					break;
				case "TestMessageDecodeKey":
					result = MessageEncodeDecode.encodeMessageKey();
					break;
				case "TestMessageDecodeMapKey640Data":
					result = MessageEncodeDecode.encodeMessageMapKey640Data();
					break;
				case "TestMessageDecodeMapKey640Keys":
					result = MessageEncodeDecode.encodeMessageMapKey640Keys();
					break;
				case "TestMessageDecodeSetKey640":
					result = MessageEncodeDecode.encodeMessageSetKey640();
					break;
				case "TestMessageDecodeSetNeighbors":
					result = MessageEncodeDecode.encodeMessageSetNeighbors();
					break;
				case "TestMessageDecodeByteBuffer":
					result = MessageEncodeDecode.encodeMessageByteBuffer();
					break;
				case "TestMessageDecodeInteger":
					result = MessageEncodeDecode.encodeMessageInt();
					break;
				case "TestMessageDecodeLong":
					result = MessageEncodeDecode.encodeMessageLong();
					break;
				case "TestMessageDecodePublicKeySignature":
					result = MessageEncodeDecode.encodeMessagePublicKeySignature();
					break;
				case "TestMessageDecodePublicKey":
					result = MessageEncodeDecode.encodeMessagePublicKey();
					break;
				case "TestMessageDecodeSetTrackerData":
					result = MessageEncodeDecode.encodeMessageSetTrackerData();
					break;
				case "TestMessageDecodeBloomFilter":
					result = MessageEncodeDecode.encodeMessageBloomFilter();
					break;
				case "TestMessageDecodeMapKey640Byte":
					result = MessageEncodeDecode.encodeMessageMapKey640Byte();
					break;
				case "TestMessageDecodeSetPeerSocket":
					result = MessageEncodeDecode.encodeMessageSetPeerSocket();
					break;
				case "TestMessageEncodeEmpty":
					result = MessageEncodeDecode.decodeMessageEmpty(argument);
					break;
				case "TestMessageEncodeKey":
					result = MessageEncodeDecode.decodeMessageKey(argument);
					break;
				case "TestMessageEncodeMapKey640Data":
					result = MessageEncodeDecode.decodeMessageMapKey640Data(argument);
					break;
				case "TestMessageEncodeMapKey640Keys":
					result = MessageEncodeDecode.decodeMessageMapKey640Keys(argument);
					break;
				case "TestMessageEncodeSetKey640":
					result = MessageEncodeDecode.decodeMessageSetKey640(argument);
					break;
				case "TestMessageEncodeSetNeighbors":
					result = MessageEncodeDecode.decodeMessageSetNeighbors(argument);
					break;
				case "TestMessageEncodeByteBuffer":
					result = MessageEncodeDecode.decodeMessageByteBuffer(argument);
					break;
				case "TestMessageEncodeLong":
					result = MessageEncodeDecode.decodeMessageLong(argument);
					break;
				case "TestMessageEncodeInteger":
					result = MessageEncodeDecode.decodeMessageInteger(argument);
					break;
				case "TestMessageEncodePublicKeySignature":
					result = MessageEncodeDecode.decodeMessagePublicKeySignature(argument);
					break;
				case "TestMessageEncodePublicKey":
					result = MessageEncodeDecode.decodeMessagePublicKey(argument);
					break;
					
				default:
					System.err.println(String.format("Invalid argument: %s.", argument));
					System.exit(-1);
					break;
			}

			InteropUtil.writeToFile(argument, result);

		} catch (Exception ex) {
			System.err.println(String.format("Exception occurred:\n%s.", ex));
			System.exit(-1);
		}

		System.exit(0);
	}
}
