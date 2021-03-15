package com.RUStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/* any necessary Java packages here */

public class RUStoreClient {

	/* any necessary class members here */
	
	private String host;
	private int port;
	private Socket socket;
	
	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here
		this.host = host;
		this.port = port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void connect() throws UnknownHostException, IOException {

		// Implement here
		socket = new Socket(host, port);
		
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, byte[] data) throws IOException {
		
		// Implement here
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write(("put:" + key + ":" + data.length).getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String response = new String(buffer, "UTF-8").trim();
		if(response.equals("serverHasKey"))
			return 1;
		if(!response.equals("putContinue"))
			throw new IOException();
		outputStream.write(data);
		return 0;

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws FileNotFoundException, IOException 
	 */
	public int put(String key, String file_path) throws FileNotFoundException, IOException {
		
		// Implement here
		File inputFile = new File(file_path);
		FileInputStream fileStream = new FileInputStream(inputFile);
		byte data[] = new byte[(int) inputFile.length()];
		fileStream.read(data);
		fileStream.close();
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write(("put:" + key + ":" + data.length).getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String response = new String(buffer, "UTF-8").trim();
		if(response.equals("serverHasKey"))
			return 1;
		if(!response.equals("putContinue"))
			throw new IOException();
		outputStream.write(data);
		return 0;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 * @throws		IOException
	 */
	public byte[] get(String key) throws IOException {
		// Implement here
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write(("get:" + key).getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String response = new String(buffer, "UTF-8").trim();
		if(response.equals("serverNoKey")) {
			return null;
		}
		else if(response.contains("serverHasKey")){
			int length = Integer.parseInt(response.split(":")[1]);
			byte[] fullBuffer = new byte[length];
			outputStream.write("getContinue".getBytes("UTF-8"));
			readSocket(fullBuffer, length);
			return fullBuffer;
		}
		else {
			throw new IOException();
		}
	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int get(String key, String file_path) throws IOException { //problematic for whatever reason
		// Implement here
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write(("get:" + key).getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		byte[] fullBuffer;
		int count = readSocket(buffer);
		String response = new String(buffer, 0, count, "UTF-8").trim();
		if(response.equals("serverNoKey")) {
			return 1;
		}
		else if(response.contains("serverHasKey")){
			int length = Integer.parseInt(response.split(":")[1]);
			fullBuffer = new byte[length];
			outputStream.write("getContinue".getBytes("UTF-8"));
			readSocket(fullBuffer, length);
		}
		else {
			throw new IOException();
		}
		
		FileOutputStream fileStream = new FileOutputStream(file_path);
		fileStream.write(fullBuffer);
		fileStream.close();
		return 0;
	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int remove(String key) throws IOException{

		// Implement here
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write(("remove:" + key).getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String response = new String(buffer, "UTF-8").trim();
		boolean isRemoved = response.equals("removeSuccess");
		if(!(isRemoved || response.equals("serverNoKey"))) {
			throw new IOException();
		}
		return isRemoved ? 0 : 1;
	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 */
	public String[] list() throws IOException {

		// Implement here
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write("list".getBytes());
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String keyList[] = new String(buffer).trim().replace("[", "").replace("]", "").split(", ");
		return keyList.length == 0 ? null : keyList;
	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {

		// Implement here
		//send server disconnect message
		OutputStream outputStream = socket.getOutputStream();
		outputStream.write("disconnect".getBytes("UTF-8"));
		byte[] buffer = new byte[8192];
		readSocket(buffer);
		String mes = new String(buffer, "UTF-8").trim();
		if(mes.equals("serverDisconnect")) {
			socket.close();
		}
		else {
			throw new IOException();
		}
		//close socket
	}
	
	private int readSocket(byte[] buffer) throws IOException {
		InputStream inputStream = socket.getInputStream();
		int length = buffer.length;
		int totalcount = 0;
		int readcount = 0;
		do {
			readcount = inputStream.read(buffer, totalcount, length - totalcount);
			totalcount += readcount;
		}while(totalcount != length && inputStream.available() != 0);
		return totalcount;
	}
	private int readSocket(byte[] buffer, int length) throws IOException {
		InputStream inputStream = socket.getInputStream();
		int totalcount = 0;
		int readcount = 0;
		do {
			readcount = inputStream.read(buffer, totalcount, length - totalcount);
			totalcount += readcount;
		}while(totalcount != length);
		return totalcount;
	}
}
