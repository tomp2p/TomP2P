package net.tomp2p;

public class Inbox {

	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("Argument missing.");
			System.err.println("Argument missing.");
			System.exit(-1);
		}

		String argument = args[0];
		
		try {
			switch(argument) {
				
				case "TestEncodeInt":
					TestClass test = new TestClass();
					//test.testDecodeInt();
					test.testEncodeInt();
					break;
				default:
					System.out.println(String.format("Invalid argument: %s.", argument));
					System.err.println(String.format("Invalid argument: %s.", argument));
					System.exit(-1);
					break;
			}
			
		} catch(Exception ex) {
			System.out.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.err.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.exit(-1);
		}
		
		System.exit(0);
	}

}
