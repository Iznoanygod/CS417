package com.RUStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Hashtable;

/* any necessary Java packages here */

public class RUStoreServer {
	/* any necessary class members here */

	public static ServerSocket serverSocket;
	
	public static Hashtable<String, byte[]> storeTable;

	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 */
	public static void main(String args[]) {

		// Check if at least one argument that is potentially a port number
		if (args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);
		
		storeTable = new Hashtable<String, byte[]>();

		// Implement here //
		
		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Server started. Listening on port " + port);
			System.out.println("Waiting for client connection...");
			while (true) {
				Socket socket = serverSocket.accept();
				System.out.println("Connection established");

				StoreThread client = new StoreThread(socket);
				client.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

class StoreThread extends Thread {

	Socket socket;

	public StoreThread(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		try {
			OutputStream outputStream = socket.getOutputStream();
			//client loop
			clientloop:
			while(true) {
				byte[] buffer = new byte[8192];
				int commandC = readSocket(socket, buffer);
				String clientCommand[] = new String(buffer, 0, commandC, "UTF-8").trim().split(":");
				switch(clientCommand[0]) {
				case "list":
					System.out.println("sending list of keys");
					String keyList = Arrays.toString(RUStoreServer.storeTable.keySet().toArray(new String[RUStoreServer.storeTable.size()]));
					outputStream.write(keyList.getBytes("UTF-8"));
					break;
				case "put":
					if(clientCommand.length == 1) {
						outputStream.write("commandError".getBytes("UTF-8"));
						break;
					}
					String pKey = clientCommand[1];
					if(RUStoreServer.storeTable.containsKey(pKey)) {
						outputStream.write("serverHasKey".getBytes("UTF-8"));
						break;
					}
					int length = Integer.parseInt(clientCommand[2]);
					outputStream.write("putContinue".getBytes("UTF-8"));
					byte fullBuffer[] = new byte[length];
					readSocket(socket, fullBuffer, length);
					System.out.println("putting key " + pKey);
					RUStoreServer.storeTable.put(pKey, fullBuffer);
					break;
				case "get":
					if(clientCommand.length == 1) {
						outputStream.write("commandError".getBytes("UTF-8"));
						break;
					}
					String key = clientCommand[1];
					if(!RUStoreServer.storeTable.containsKey(key)) {
						outputStream.write("serverNoKey".getBytes("UTF-8"));
						break;
					}
					System.out.println("getting key " + key);
					byte data[] = RUStoreServer.storeTable.get(key);
					outputStream.write(("serverHasKey:" + data.length).getBytes("UTF-8"));
					int count = readSocket(socket, buffer);
					if((new String(buffer, 0, count, "UTF-8")).trim().equals("getContinue")) {
						outputStream.write(data);
					}
					break;
				case "remove":
					if(clientCommand.length == 1) {
						outputStream.write("commandError".getBytes("UTF-8"));
						break;
					}
					String rKey = clientCommand[1];
					if(!RUStoreServer.storeTable.containsKey(rKey)) {
						outputStream.write("serverNoKey".getBytes("UTF-8"));
						break;
					}
					System.out.println("removing key " + rKey);
					RUStoreServer.storeTable.remove(rKey);
					outputStream.write("removeSuccess".getBytes("UTF-8"));
					break;
				case "disconnect":
					System.out.println("disconnecting client");
					outputStream.write("serverDisconnect".getBytes("UTF-8"));
					socket.close();
					break clientloop;
				default:
					outputStream.write("commandError".getBytes("UTF-8"));
					break;
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int readSocket(Socket s, byte[] buffer) throws IOException {
		InputStream inputStream = s.getInputStream();
		int length = buffer.length;
		int totalcount = 0;
		int readcount = 0;
		do {
			readcount = inputStream.read(buffer, totalcount, length - totalcount);
			totalcount += readcount;
		}while(totalcount != length && inputStream.available() != 0);
		return totalcount;
	}
	public static int readSocket(Socket s, byte[] buffer, int length) throws IOException {
		InputStream inputStream = s.getInputStream();
		int totalcount = 0;
		int readcount = 0;
		do {
			readcount = inputStream.read(buffer, totalcount, length - totalcount);
			totalcount += readcount;
		}while(totalcount != length && inputStream.available() != 0);
		return totalcount;
	}
}