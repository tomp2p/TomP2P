package net.tomp2p;

public class Inbox {

	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("Argument missing.");
			System.err.println("Argument missing.");
			System.exit(-1);
		}

		String argument = args[0];
		System.out.print(String.format("Argument: %s ", argument));
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
				case "TestMessageDecodeInt":
					result = MessageEncodeDecode.encodeMessageInt();
					break;
				case "TestMessageDecodeLong":
					result = MessageEncodeDecode.encodeMessageLong();
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
				
				default:
					System.out.println(String.format("Invalid argument: %s.", argument));
					System.err.println(String.format("Invalid argument: %s.", argument));
					System.exit(-1);
					break;
			}

			InteropUtil.writeToFile(argument, result);

		} catch (Exception ex) {
			System.out.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.err.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.exit(-1);
		}

		System.exit(0);
	}

}
