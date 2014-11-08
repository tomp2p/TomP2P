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
					result = DotNetEncodeDecodeTest.testDecodeInt(argument);
					break;
				case "TestDecodeInt":
					result = DotNetEncodeDecodeTest.encodeInt();
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
