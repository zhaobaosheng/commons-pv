package com.zdawn.commons.pv;

import java.io.UnsupportedEncodingException;

public class StringMessage implements Message {
	private String body;
	//charset
	private String encoding = "utf-8";
	
	public StringMessage(String data){
		this.body = data;
	}
	public StringMessage(){
	}
	@Override
	public byte[] exportMessage() {
		if(encoding==null || "".equals(encoding)) encoding="utf-8";
		byte[] data = null;
		try {
			data=body.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return data;
	}
	@Override
	public Message importMessage(byte[] data) {
		if(encoding==null || "".equals(encoding)) encoding="utf-8";
		try {
			this.body = new String(data, encoding);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return this;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	public String getBody() {
		return body;
	}
}
