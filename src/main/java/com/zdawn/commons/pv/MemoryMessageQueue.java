package com.zdawn.commons.pv;

import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 内存消息队列
 * <br>不支持消息持久化
 * <br>消息被处理一次后丢弃
 */
public class MemoryMessageQueue implements MessageQueue<StringMessage> {
	private static Logger logger = LoggerFactory.getLogger(MemoryMessageQueue.class);
	/**
	 * 队列最大深度
	 */
	private int maxSize = 200;
	/**
	 * 队列编号
	 */
	private String queueNo;
	/**
	 * message 队列
	 */
	private LinkedList<MessageWrapper<StringMessage>> queueMsg = new LinkedList<>();
	/**
	 * 监控类
	 */
	private Superviser superviser;
	
	@Override
	public synchronized boolean putMessage(StringMessage msg) {
		if(superviser.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize) return false;
		MessageWrapper<StringMessage> wrapper = new MessageWrapper<StringMessage>(msg);
		queueMsg.add(wrapper);
		notify();
		return true;
	}

	@Override
	public synchronized MessageWrapper<StringMessage> pollMessage() {
		try {
			while(queueMsg.size()==0){
				wait();
			}
		} catch (InterruptedException e) {
			logger.error("pollMessage",e);
		}
		return queueMsg.removeFirst();
	}

	@Override
	public boolean canAddMessage() {
		return queueMsg.size() < maxSize;
	}

	@Override
	public int getCurrentQueueSize() {
		return queueMsg.size();
	}

	@Override
	public int getMaxSize() {
		return maxSize;
	}

	@Override
	public void init(Map<String, String> para) {
		//初始化参数
		String temp = para.get("queueMaxSize");
		if(temp!=null && !"".equals(temp)) {
			maxSize = Integer.parseInt(temp);
		}
		temp = para.get("queueNo");
		if(temp ==null || "".equals(temp)) throw new RuntimeException("queueNo 未设置");
		queueNo = temp;
	}

	@Override
	public void loadPendingMsgToQueue() {
	}

	@Override
	public void onHandleMsgResult(int result, MessageWrapper<StringMessage> msgWrapper) {
		//如果 1 success 0 error not dispose
		try {
			if(result==2) saveMsgLog(msgWrapper.getMsg(), 2);
		} catch (Exception e) {
			logger.error("onHandleMsgResult",e);
		}
	}

	@Override
	public void setSuperviser(Superviser superviser) {
		this.superviser = superviser;
	}

	@Override
	public void saveMsgLog(StringMessage msg, int msgSource) throws Exception {
		logger.info("queueNo="+queueNo+" msgSource="+msgSource+" msg="+msg.getPayload());
	}
}
