package com.sinosoft.ie.commons.pv;
/**
 * message queue
 * cache waiting dispose message
 * @author zhaobaosheng
 * 2018-01-11
 */
public interface MessageQueue {
	/**
	 * add message
	 * @param Message
	 * @return true adding success false adding failture
	 */
	public boolean putMessage(Message msg);
	/**
	 * get MessageWrapper from queue
	 * MessageWrapper can get Message
	 */
	public MessageWrapper pollMessage();
	/**
	 * whether or no add message
	 */
	public boolean canAddMessage();
	/**
	 * gain current queue size
	 */
	public int getCurrentQueueSize();
	/**
	 * gain queue max size
	 */
	public int getMaxSize();
	/**
	 * initialize MessageQueue before working it should be invoke
	 */
	public void init();
	/**
	 * load pending message put into queue, so the message will be dispose.
	 */
	public void loadPendingMsgToQueue();
	/**
	 * the method should be invoke in order to MessageQueue can delete the message of handing successfully
	 * or can dispose processing failed message
	 */
	public void onHandleMsgResult(boolean success,MessageWrapper msgWrapper);
	/**
	 * config HandlingMediator
	 * in order to communication with other class
	 * it should be set before init method  invoking
	 */
	public void configureMediator(HandlingMediator mediator);
}
