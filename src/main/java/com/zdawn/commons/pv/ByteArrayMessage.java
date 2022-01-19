package com.zdawn.commons.pv;

public class ByteArrayMessage implements Message<byte[]> {
	/**
	 * message unique identification
	 */
	private String messageId;
	/**
	 * hashKey needed when sending message orderly
	 */
	private String hashKey;
	
	private byte[] body;
	
	public ByteArrayMessage(byte[] data){
		this.body = data;
	}
	
	public ByteArrayMessage(String hashKey,byte[] data){
		this.hashKey = hashKey;
		this.body = data;
	}

	public ByteArrayMessage(){
	}

	@Override
	public byte[] getPayload() {
		return body;
	}

	@Override
	public String getMessageId() {
		return messageId;
	}

	@Override
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	@Override
	public String getHashKey() {
		return hashKey;
	}

	public void setHashKey(String hashKey) {
		this.hashKey = hashKey;
	}

	public void setPayload(byte[] body) {
		this.body = body;
	}
}
