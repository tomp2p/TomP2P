package net.tomp2p;

public class Inbox {

	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("Argument missing.");
			System.err.println("Argument missing.");
			//System.exit(-1); // TODO uncomment (3x)
		}

		String argument = args[0];
		
		try {
			switch(argument) {
				
				case "TestEncodeInt":
					TestDotNetInterop test = new TestDotNetInterop();
					test.testDecodeInt();
					
					break;
				default:
					System.out.println(String.format("Invalid argument: %s.", argument));
					System.err.println(String.format("Invalid argument: %s.", argument));
					//System.exit(-1);
					break;
			}
			
		} catch(Exception ex) {
			System.out.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			System.err.println(String.format("Exception occurred: %s\n%s.", ex.getCause(), ex));
			//System.exit(-1);
		}
	}

}
