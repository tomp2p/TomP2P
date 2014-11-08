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

		Boolean result = null;

		try {
			// executed .NET test
			switch (argument) {

				// primitive type encoding / decoding
				case "TestEncodeInt":
					result = DotNetEncodeDecodeTest.testDecodeInt(argument);
					break;
					
					
				default:
					System.out.println(String.format("Invalid argument: %s.", argument));
					System.err.println(String.format("Invalid argument: %s.", argument));
					System.exit(-1);
					break;
			}

			System.out.print(String.format("--> Result: %s\n", result.toString().toUpperCase()));

			byte[] resultBytes = new byte[1];
			resultBytes[0] = result ? (byte) 1 : (byte) 0;
			
			InteropUtil.writeToFile(argument, resultBytes);

		} catch (Exception ex) {
			System.out.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.err.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.exit(-1);
		}

		System.exit(0);
	}

}
