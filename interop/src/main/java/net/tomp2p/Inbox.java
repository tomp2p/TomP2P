package net.tomp2p;

import java.io.IOException;
import java.util.Scanner;

public class Inbox {

	private static boolean exit = false;

	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			System.err.println("Argument missing.");
			System.exit(-1);
		}
		String argument = args[0];
		
		while (!exit) {
			exit = true;
			System.out.println(String.format("Argument: %s ", argument));

			try {
				execute(argument);
			} catch (Exception ex) {
				System.err.println(String.format("Exception occurred:\n%s.", ex));
				System.exit(-1);
			}			
			
			System.out.println("Waiting for next argument...");
			Scanner scanner = new Scanner(System.in);
			argument = scanner.nextLine();
			scanner.close();
		}

		System.out.println("Exiting.");
		System.exit(0);
	}

	private static void execute(String argument) throws Exception {
		byte[] result = null;
		// executed .NET test
		switch (argument) {

		// primitive type encoding / decoding
			case "TestEncodeByte":
				result = DotNetEncodeDecode.testDecodeByte(argument);
				break;
			case "TestEncodeBytes":
				result = DotNetEncodeDecode.testDecodeBytes(argument);
				break;
			case "TestEncodeShort":
				result = DotNetEncodeDecode.testDecodeShort(argument);
				break;
			case "TestEncodeInt":
				result = DotNetEncodeDecode.testDecodeInt(argument);
				break;
			case "TestEncodeLong":
				result = DotNetEncodeDecode.testDecodeLong(argument);
				break;
			case "TestDecodeByte":
				result = DotNetEncodeDecode.encodeByte();
				break;
			case "TestDecodeBytes":
				result = DotNetEncodeDecode.encodeBytes();
				break;
			case "TestDecodeShort":
				result = DotNetEncodeDecode.encodeShort();
				break;
			case "TestDecodeInt":
				result = DotNetEncodeDecode.encodeInt();
				break;
			case "TestDecodeLong":
				result = DotNetEncodeDecode.encodeLong();
				break;

			// Message object encoding / decoding
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
			case "TestMessageEncodeSetTrackerData":
				result = MessageEncodeDecode.decodeMessageSetTrackerData(argument);
				break;
			case "TestMessageEncodeBloomFilter":
				result = MessageEncodeDecode.decodeMessageBloomFilter(argument);
				break;
			case "TestMessageEncodeMapKey640Byte":
				result = MessageEncodeDecode.decodeMessageMapKey640Byte(argument);
				break;
			case "TestMessageEncodeSetPeerSocket":
				result = MessageEncodeDecode.decodeMessageSetPeerSocket(argument);
				break;
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

			// pings
			case "TestPingJavaUdp-start":
				exit = false;
				Pings.startJavaPingReceiver();
				break;
			case "TestPingJavaUdp-stop":
				Pings.stopJavaPingReceiver();
				break;
				
			default:
				System.err.println(String.format("Invalid argument: %s.", argument));
				System.exit(-1);
				break;
		}

		InteropUtil.writeToFile(argument, result);
	}
}
