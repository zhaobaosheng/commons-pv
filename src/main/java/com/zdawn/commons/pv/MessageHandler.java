package com.zdawn.commons.pv;
/**
 * dispose message interface
 * @author zhaobaosheng
 * 2022-01-16
 */
public interface MessageHandler {
	/**
	 * 处理消息 如果抛出Exception则认为处理消息失败
	 * @param msg 消息
	 * @return 1 消息处理成功 0 处理失败 2消息不能处理
	 * @throws Exception
	 */
	public int handleMessage(StringMessage msg) throws Exception;
}
