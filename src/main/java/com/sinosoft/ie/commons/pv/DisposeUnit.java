package com.sinosoft.ie.commons.pv;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * support add message
 * support use thread pool process message
 * @author zhaobs
 * @date 2018-01-15
 */
public class DisposeUnit {
	private static Logger log = LoggerFactory.getLogger(DisposeUnit.class);
	/**
	 * 消息队列
	 */
	private MessageQueue msgQueue = null;
	/**
	 * 组件状态
	 * 1 初始
	 * 2 运行中
	 * 3 停止
	 */
	private int status = 1;
	
	private MsgBroker msgBroker = null;
	
	private MessageHandler messageHandler = null;
	
	private Superviser superviser;
	
	private HandlingMediator mediator;
	/**
	 * 添加消息，添加不成功返回false
	 */
	public boolean addMessage(Message msg){
		if(status!=2){
			log.warn("DisposeUnit is not running status="+status);
			return false;
		}
		return msgQueue.putMessage(msg);
	}
	/**
	 * 添加消息,如果添加失败,阻塞等待,直到添加成功为止
	 * @param msg 消息
	 * @param waitSpanTime 重试添加消息的等待时间-单位毫秒
	 */
	public void addMessage(Message msg,long waitSpanTime){
		boolean success = addMessage(msg);
		while(!success){
			try {
				Thread.sleep(waitSpanTime);
			} catch (InterruptedException e) {}
			success = addMessage(msg);
		}
	}
	/**
	 * 当前处理单元队列深度
	 */
	public int getCurrentTaskQueueSize(){
		return msgQueue.getCurrentQueueSize();
	}
	/**
	 * 可利用队列深度,数值越大可放任务越多
	 */
	public int availableTaskQueueSize(){
		return msgQueue.getMaxSize()-msgQueue.getCurrentQueueSize();
	}
	
	public float getQueueUsingPercent(){
		return msgQueue.getCurrentQueueSize()/msgQueue.getMaxSize();
	}
	/**
	 * 组件初始化,初始化完成即可处理消息
	 */
	public void init(){
		if(this.status > 1){
			log.warn("DisposeUnit status is invalid");
			return;
		}
		if(msgQueue==null) throw new RuntimeException("MessageQueue is not setting");
		if(msgBroker==null) throw new RuntimeException("MsgBroker is not setting");
		if(messageHandler==null) throw new RuntimeException("MessageHandler is not setting");
		if(superviser==null) throw new RuntimeException("Superviser is not setting");
		mediator = new HandlingMediator();
		mediator.setMessageHandler(messageHandler);
		mediator.setMsgQueue(msgQueue);
		mediator.setSuperviser(superviser);
		msgBroker.configureMediator(mediator);
		msgBroker.init();
		msgQueue.configureMediator(mediator);
		msgQueue.init();
		superviser.init();
		Runtime.getRuntime().addShutdownHook(new HookThread());
		this.status = 2;
	}
	class HookThread extends Thread {
		@Override
		public void run() {
			stopHandling();
		}
	}
	/**
	 * 组件停止处理消息
	 */
	public void stopHandling (){
		this.status = 3;
		this.msgBroker.stopWorking();
		log.info("DisposeUnit already stop");
	}
	/**
	 * 加载上一次组件停止后未处理的消息
	 * 或是执行失败消息再次执行
	 */
	public void loadPendingMessage(){
		log.info("DisposeUnit load pending message now");
		msgQueue.loadPendingMsgToQueue();
	}

	public Map<String,Object> getMonitorInfoSnapshot(){
		Map<String,Object> snap = superviser.getMonitorInfoSnapshot();
		snap.put("msgQueueCurrentSize", msgQueue==null?0:msgQueue.getCurrentQueueSize());
		snap.put("msgQueueMaxSize", msgQueue==null?0:msgQueue.getMaxSize());
		return snap;
	}
	

	public MessageQueue getMsgQueue() {
		return msgQueue;
	}
	
	public void setMsgQueue(MessageQueue msgQueue) {
		this.msgQueue = msgQueue;
	}
	
	public void setMsgBroker(MsgBroker msgBroker) {
		this.msgBroker = msgBroker;
	}

	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}
	
	public void setSuperviser(Superviser superviser) {
		this.superviser = superviser;
	}
}
