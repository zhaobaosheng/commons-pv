package com.zdawn.commons.pv;
/**
 * 协调类之间协作
 * @author zhaobaosheng
 * 2018-01-18
 */
public class HandlingMediator {
	/**
	 * 消息队列
	 */
	private MessageQueue msgQueue;
	/**
	 * 监管
	 */
	private Superviser superviser;
	/**
	 * 消息处理接口
	 */
	private MessageHandler messageHandler;
	
	public void informMessageHandleResult(long startHandlingTime,long spendTime,
			boolean success,MessageWrapper msgWrapper){
		superviser.collectMessageHandleResult(startHandlingTime, spendTime, success);
		msgQueue.onHandleMsgResult(success, msgWrapper);
	}
	
	public void informMessageRejection(long startHandlingTime,long spendTime){
		superviser.collectMessageRejection(startHandlingTime, spendTime);
	}
	/**
	 * to see Superviser.getBreakerStatus()
	 */
	public int getBreakerStatus(){
		return superviser.getBreakerStatus();
	}
	/**
	 * to see MessageQueue.pollMessage()
	 */
	public MessageWrapper pollMessage(){
		return msgQueue.pollMessage();
	}
	/**
	 * to see Superviser.forceCloseBreaker()
	 */
	public void forceCloseBreaker(){
		superviser.forceCloseBreaker();
	}
	
	public MessageHandler getMessageHandler(){
		return messageHandler;
	}
	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}
	public void setMsgQueue(MessageQueue msgQueue) {
		this.msgQueue = msgQueue;
	}
	public void setSuperviser(Superviser superviser) {
		this.superviser = superviser;
	}
}
