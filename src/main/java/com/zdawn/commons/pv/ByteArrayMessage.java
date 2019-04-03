package com.zdawn.commons.pv;

public class ByteArrayMessage implements Message {
	private byte[] body;
	
	public ByteArrayMessage(byte[] data){
		this.body = data;
	}

	public ByteArrayMessage(){
	}
	@Override
	public byte[] exportMessage() {
		return body;
	}

	@Override
	public Message importMessage(byte[] data) {
		this.body = data;
		return this;
	}
}
