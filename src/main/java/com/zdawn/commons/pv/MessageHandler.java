package com.zdawn.commons.pv;
/**
 * dispose message interface
 * @author zhaobaosheng
 * 2018-01-12
 */
public interface MessageHandler {
	/**
	 * 处理消息 如果抛出Exception则认为处理消息失败
	 * @param Message
	 * @throws Exception
	 */
	public void handleMessage(Message msg) throws Exception;
}
