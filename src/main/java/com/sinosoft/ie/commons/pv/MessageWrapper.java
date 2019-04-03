package com.sinosoft.ie.commons.pv;

import java.util.HashMap;
import java.util.Map;
/**
 * 消息包装类
 * @author zhaobaosheng
 * 2018-01-12
 */
public class MessageWrapper {
	/**
	 * 消息
	 */
	private Message msg;
	/**
	 * 消息上下文参数
	 */
	private Map<String,Object> context = new HashMap<String,Object>();
	
	public MessageWrapper(Message msg){
		this.msg = msg;
	}
	
	public Message getMsg() {
		return msg;
	}

	public void putAttribute(String key,Object attri){
		context.put(key,attri);
	}
	
	public Object getAttribute(String key){
		return context.get(key);
	}

	public Map<String, Object> getContext() {
		return context;
	}

	public void setContext(Map<String, Object> context) {
		this.context = context;
	}
}
