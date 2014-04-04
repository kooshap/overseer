package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class overseer_client extends DB {

    private static final int OK = 0;
    private static final int SERVER_ERROR = 1;
    private static final int CLIENT_ERROR = 2;
    public static final String HOST_PROPERTY = "overseer.host";
    public static final String PORT_PROPERTY = "overseer.port";
    public static final int DEFAULT_PORT = 5555;
    
    private Socket echoSocket;
	private PrintWriter out;
	private BufferedReader in;
	private BufferedReader stdIn;
			
	long maxInt=Integer.MAX_VALUE;
	
	public void init() throws DBException {
		Properties props = getProperties();
        int port;

		try {
			InetAddress address = InetAddress.getByName(props.getProperty(HOST_PROPERTY));
	        String portString = props.getProperty(PORT_PROPERTY);
	        if (portString != null) {
	            port = Integer.parseInt(portString);
	        }
	        else {
	            port = DEFAULT_PORT;
	        }			
	        
	        //DatagramSocket socket = new DatagramSocket();
			Socket socket = new Socket(address, port);
			//socket.setSoTimeout(1000);
			out = new PrintWriter(socket.getOutputStream(),	true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			stdIn = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Connected to overseer.");

		} catch (Exception e) {
			System.out.println(e.toString());
		} finally {

		}
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		out.println("r " + hash(key));
		out.flush();
		String buf;
		try {
			buf=in.readLine();
			if (buf.length()>0)
				result.put(key,new StringByteIterator(buf));
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return result.isEmpty() ? SERVER_ERROR : OK;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		//System.out.println("w " + key + " " + values.entrySet().iterator().next().getValue());
		out.println("w " + hash(key) + " " + values.entrySet().iterator().next().getValue());
		out.flush();
		String buf;
		try {
			buf=in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return OK;
	}

	@Override
	public int delete(String table, String key) {
		out.println("d " + hash(key));
		out.flush();
		String buf;
		try {
			buf=in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return OK;
	}

	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		out.println("w " + hash(key) + " " + values.entrySet().iterator().next().getValue());
		out.flush();
		String buf;
		try {
			buf=in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return OK;
	}

    private long hash(String key) {
    	long keyHash=key.hashCode()+maxInt+1L;
        return keyHash;
    }
}
