package com.RUStore;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class TestStringCLI {

	private static RUStoreClient client;
	private static BufferedReader userdata;
	private static String line;
	private static String command;
	private static List<String> arguments;
	private static Matcher m;

	public static void printWelcome() {
		System.out.println("\nThis little client utilizes a uses an RUClient to allow\n" +
				"you to send and store strings within an object store.");
	}

	public static void printUsage() {
		System.out.println("\nUsage:");
		System.out.println("   connect <host> <port>");
		System.out.println("   put <key> <string>");
		System.out.println("   get <key>");
		System.out.println("   remove <key>");
		System.out.println("   list");
		System.out.println("   disconnect");
		System.out.println("   exit\n");
	}


	static void connect () {
		// Arguments check
		if(arguments.size() != 3) {
			System.out.println("Bad arguments. Host and port must be specified.");
			printUsage();
			return;
		}

		// Create new client
		client = new RUStoreClient(arguments.get(1), Integer.parseInt(arguments.get(2)));

		// Connect to object server
		System.out.println("Connecting to server at " + arguments.get(1) + ":" + arguments.get(2) + "...");    	
		try {
			client.connect();
			System.out.println("Connection established.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Connection failed. Exception occured!");
		}

	}

	static void put() {
		int ret = -1;

		// Arguments check
		if(arguments.size() != 3) {
			System.out.println("Bad arguments. Key and String must be specified.");
			printUsage();
			return;
		}

		// Put string with key
		System.out.println("Putting string: \"" + arguments.get(2) + "\" with key \"" + arguments.get(1) + "\"...");
		try {
			ret = client.put(arguments.get(1), arguments.get(2).getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to put " + arguments.get(1) + ". Exception occured!");
			return;
		}

		// Check returns
		if(ret == 0) {
			System.out.println("Successfully put " + arguments.get(1));
		}else {
			System.out.println("Failed to put " + arguments.get(1) + ". (key already exists)");
		}
	}

	static void get() {
		// Arguments check
		if(arguments.size() != 2) {
			System.out.println("Bad arguments. Key must be specified.");
			printUsage();
			return;
		}

		// Get string with key
		byte[] bytes; 
		System.out.println("Getting string with key \"" + arguments.get(1) + "\"...");

		try {
			bytes = client.get(arguments.get(1));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to get string with key " + arguments.get(1) + ". Exception occured!");
			return;
		}

		// Check return
		if(bytes != null) {
			System.out.println("Successfully received string.");
			System.out.println("Received string: \"" + new String(bytes) + "\"");
		}else {
			System.out.println("Failed to get string with key " + arguments.get(1) + " (key does not exist)");
		}
	}

	static void remove() {
		int ret = -1;

		// Arguments check
		if(arguments.size() != 2) {
			System.out.println("Bad arguments. Key must be specified.");
			printUsage();
			return;
		}

		// Remove string with key
		System.out.println("Removing object with key " + arguments.get(1) + "...");
		try {
			ret = client.remove(arguments.get(1));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to remove object with key " + arguments.get(1) + ". Exception occured!");
		}

		// Check return
		if(ret == 0) {
			System.out.println("Successfully removed object with key " + arguments.get(1));
		}else {
			System.out.println("Failed to remove object with key " + arguments.get(1) + " (key does not exist)");
		}
	}

	static void list () {
		// Retrieve list of keys
		System.out.println("Going to get object keys...");
		String[] keys;
		int index;

		try {
			keys = client.list();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to retrieve keys. Exception occured!");
			return;
		}

		// Check Return
		if (keys != null) {
			System.out.println("Successfully retrieved keys");
			System.out.print("Object Keys: ");
			for(index = 0; index < keys.length - 1; index++) {
				System.out.print(keys[index] + ", ");
			}
			System.out.print(keys[index] + "\n");
		} else {
			System.out.println("No available keys");
		}
	}

	static void disconnect () {

		// Disconnect
		System.out.println("Disconnecting from server...");
		try {
			client.disconnect();
			System.out.println("Sucessfully disconnected from server.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to disconnect from server. Exception occured!");
		}

	}


	public static void main(String[] args) throws Exception {
		// Setup variables
		client = null;
		userdata = new BufferedReader(new InputStreamReader(System.in));

		// Print startup messages
		printWelcome();
		printUsage();

		// Main program loop
		while(true) {
			// Read in user command
			System.out.print("> ");
			line = userdata.readLine();

			// Parse command and arguments	        
			arguments = new ArrayList<String>();
			m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
			while (m.find())
				arguments.add(m.group(1).replace("\"", ""));

			// Carry out command
			command = arguments.get(0);
			if(command.equals("connect") ){
				connect();
			}else if(command.equals("exit")) {
				break;
			}else {
				if(client == null) {
					System.out.println("No Client Available. Have you connected via the \"connect\" command?");
					continue;
				}
				if(command.equals("put")) {
					put();
				}else if(command.equals("get")) {
					get();
				}else if(command.equals("remove")) {
					remove();
				}else if(command.equals("list")) {
					list();
				}else if(command.equals("disconnect")) {
					disconnect();
				}else{
					System.out.println("Invalid Command.");
					printUsage();
				}	      	
			}	
		}
		userdata.close();
	}

}
