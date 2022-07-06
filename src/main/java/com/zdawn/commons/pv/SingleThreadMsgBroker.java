package com.zdawn.commons.pv;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单线程消费消息
 */
public class SingleThreadMsgBroker extends Thread implements MsgBroker{
	private static Logger log = LoggerFactory.getLogger(SingleThreadMsgBroker.class);
	/**
	 * start or stop
	 */
	private boolean startBroker = true;
	/**
	 * message queue
	 */
	private MessageQueue<StringMessage> messageQueue;
	/**
	 * 处理消息实现类
	 */
	private MessageHandler messageHandler;
	/**
	 * 监控类
	 */
	private Superviser superviser;
    /**
     * broker标识
     */
    private String brokerTag;
	
	@Override
	public void stopWorking() {
		startBroker = false;
	}
	
	public void run() {
		while(startBroker)
		{
			MessageWrapper<StringMessage> msgWrapper = messageQueue.pollMessage();
			if(msgWrapper!=null){
				try {
					handleMessage(msgWrapper);
				} catch (Exception e) {
					log.error("invoke handleMessage error",e);
				}
			}else {
				log.warn("it is null message");
			}
		}
	}
	/**
	 * 初始化方法
	 */
	public void init(){
		if(messageQueue==null) throw new RuntimeException("MessageQueue is not setting");
		if(messageHandler==null) throw new RuntimeException("MessageHandler is not setting");
		if(superviser==null) throw new RuntimeException("Superviser is not setting");
		this.start();
	}
	/**
	 * 处理消息方法
	 * 调用消息实现类接口
	 */
	public void handleMessage(MessageWrapper<StringMessage> msgWrapper) {
		int status = superviser.getBreakerStatus();
		if(status==0 || status==2){// not open
			handleMsg(status, msgWrapper);
		}else if(status==1){//open
			while(true) {
				try {
					log.warn("breaker status is open. Now wait next period to do!");
					Thread.sleep(superviser.getPauseMessageProcessingTime());
				} catch (InterruptedException e) {}
				//get breaker status again
				status = superviser.getBreakerStatus();
				if(status != 1) break;
			}
			handleMsg(status, msgWrapper);
		}
	}
	
	private void handleMsg(int status,MessageWrapper<StringMessage> msgWrapper) {
		long start = System.currentTimeMillis();
		try {
			//调用处理消息方法
			int result = messageHandler.handleMessage(msgWrapper.getMsg());
			long end  = System.currentTimeMillis();
			if(result==1) {//处理成功
				messageQueue.onHandleMsgResult(1, msgWrapper);
				superviser.collectMessageHandleResult(start,end-start,true);
				//semi-open reset breaker status
				if(status==2) superviser.forceCloseBreaker();
			}else if(result==0) {//处理失败
				messageQueue.onHandleMsgResult(0, msgWrapper);
				superviser.collectMessageHandleResult(start,end-start,false);
			}else {//消息
				messageQueue.onHandleMsgResult(2, msgWrapper);
				superviser.collectMessageRejection(start, end-start);
			}
		} catch (Exception e) {
			//error
			long end  = System.currentTimeMillis();
			messageQueue.onHandleMsgResult(0, msgWrapper);
			superviser.collectMessageHandleResult(start,end-start,false);
			log.error("invoke MessageHandler.handleMessage",e);
		}
	}
	
	@Override
	public Map<String, Object> getMonitorInfoSnapshot() {
		Map<String, Object> snapshot = superviser.getMonitorInfoSnapshot();
		snapshot.put("brokerTag", brokerTag);
		snapshot.put("queueMaxSize", messageQueue.getMaxSize());
		snapshot.put("queueCurrentSize", messageQueue.getCurrentQueueSize());
		return snapshot;
	}

	public void setMessageQueue(MessageQueue<StringMessage> messageQueue) {
		this.messageQueue = messageQueue;
	}

	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	public void setSuperviser(Superviser superviser) {
		this.superviser = superviser;
	}

	public void setBrokerTag(String brokerTag) {
		this.brokerTag = brokerTag;
	}
}
