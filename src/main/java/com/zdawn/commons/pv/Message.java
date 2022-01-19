package com.zdawn.commons.pv;
/**
 * message interface
 * 实现类必须实现空构造方法
 * @author zhaobaosheng
 * 2022-01-16
 */
public interface Message<T> {
	/**
	 * return message body
	 */
	T getPayload();
	/**
	 * message unique identification
	 */
	public String getMessageId();
	public void setMessageId(String messageId);
	/**
	 * hashKey needed when sending message orderly
	 */
	public String getHashKey();
}
