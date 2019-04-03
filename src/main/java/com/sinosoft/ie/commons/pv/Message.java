package com.sinosoft.ie.commons.pv;
/**
 * message interface
 * 实现类必须实现空构造方法
 * @author zhaobaosheng
 * 2018-01-11
 */
public interface Message {
	/**
	 * export message content
	 * if this method throw RuntimeException message is corrupt 
	 */
	public byte[] exportMessage();
	/**
	 * load message
	 * if this method throw RuntimeException message is corrupt
	 */
	public Message importMessage(byte[] data);
}
