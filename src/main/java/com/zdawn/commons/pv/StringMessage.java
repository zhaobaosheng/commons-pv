package com.zdawn.commons.pv;

public class StringMessage implements Message<String> {
	/**
	 * message unique identification
	 */
	private String messageId;
	/**
	 * hashKey needed when sending message orderly
	 */
	private String hashKey;
	/**
	 * payload
	 */
	private String payload;
	
	public StringMessage(String data){
		this.payload = data;
	}
	public StringMessage(String data,String hashKey){
		this.payload = data;
		this.hashKey = hashKey;
	}
	public StringMessage(){
	}
	
	@Override
	public String getPayload() {
		return payload;
	}
	@Override
	public String getMessageId() {
		return messageId;
	}
	@Override
	public String getHashKey() {
		return hashKey;
	}
	
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
	public void setHashKey(String hashKey) {
		this.hashKey = hashKey;
	}
	public void setPayload(String payload) {
		this.payload = payload;
	}
}
