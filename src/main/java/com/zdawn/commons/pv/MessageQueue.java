package com.zdawn.commons.pv;

import java.util.Map;

/**
 * message queue
 * cache waiting dispose message
 * @author zhaobaosheng
 * 2018-01-11
 */
public interface MessageQueue<T> {
	/**
	 * add message
	 * @param Message
	 * @return true adding success false adding failture
	 */
	public boolean putMessage(T msg);
	/**
	 * get MessageWrapper from queue
	 * MessageWrapper can get Message
	 */
	public MessageWrapper<T> pollMessage();
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
	public void init(Map<String,String> para);
	/**
	 * load pending message put into queue, so the message will be dispose.
	 */
	public void loadPendingMsgToQueue();
	/**
	 * the method should be invoke in order to MessageQueue can delete the message of handing successfully
	 * or can dispose processing failed message
	 * @param result 1 success 0 error 2 rejection
	 */
	public void onHandleMsgResult(int result,MessageWrapper<T> msgWrapper);
	
	public void setSuperviser(Superviser superviser);
}
