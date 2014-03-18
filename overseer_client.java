package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class overseer_client extends DB {

	private Socket echoSocket;
	private PrintWriter out;
	private BufferedReader in;
	private BufferedReader stdIn;

	public void init() throws DBException {
		try {
			Socket echoSocket = new Socket("127.0.0.1", 5000);
			out = new PrintWriter(echoSocket.getOutputStream(),	true);
			in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
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
		out.println("r " + key);
		String res="";
		try {
			res = in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return Integer.parseInt(res);
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		out.println("w " + key);
		String res="";
		try {
			res = in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return Integer.parseInt(res);
	}

	@Override
	public int delete(String table, String key) {
		out.println("d " + key);
		String res="";
		try {
			res = in.readLine();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return Integer.parseInt(res);
	}

	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return 0;
	}

}
