package com.zdawn.commons.pv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * support add message
 * support use thread pool process message
 * @author zhaobs
 * @date 2022-01-17
 */
public class DisposeUnit {
	private static Logger log = LoggerFactory.getLogger(DisposeUnit.class);
	/**
	 * 组件状态
	 * 1 初始
	 * 2 运行中
	 * 3 停止
	 */
	private int status = 1;
	/**
	 * 处理线程个数
	 */
	private int handleThreadCount = 2;
	/**
	 * 消息队列实现类
	 */
	private String messageQueueClazzName;
	/**
	 * 处理单元标识
	 */
	private String disposeUnitTag;
	/**
	 * 配置参数
	 */
	private Map<String,String> para;
	/**
	 * 消息处理实现类
	 */
	private MessageHandler messageHandler = null;
	/**
	 * 存储消息数据库源  com.zdawn.commons.pv.JdbcMessageQueue 需要设置
	 */
	private DataSource dataSource; 
	/**
	 * 消息队列
	 */
	private List<MessageQueue<StringMessage>> msgQueueList = null;
	
	private List<SingleThreadMsgBroker> msgBrokerList = null;
	
	private ConsistencyHash hashRouter;
	/**
	 * 添加消息，添加不成功返回false
	 */
	public boolean addMessage(StringMessage msg){
		boolean result = false;
		if(status!=2){
			log.warn("DisposeUnit is not running status="+status);
			return false;
		}
		if(msg.getHashKey()==null || "".equals(msg.getHashKey())) {
			//随机put消息
			int index = generateRandom(msgQueueList.size());
			MessageQueue<StringMessage> queue = msgQueueList.get(index);
			result = queue.putMessage(msg);
		}else {//hash put消息
			int index = hashRouter.selectHashQueueIndex(msg.getHashKey());
			MessageQueue<StringMessage> queue = msgQueueList.get(index);
			result = queue.putMessage(msg);
		}
		return result;
	}
	//Random bound exclusive
	private int generateRandom(int bound) {
		Random rand = new Random();
		return rand.nextInt(bound);
	}
	/**
	 * 添加消息,如果添加失败,阻塞等待,直到添加成功为止
	 * @param msg 消息
	 * @param waitSpanTime 重试添加消息的等待时间-单位毫秒
	 */
	public void addMessage(StringMessage msg,long waitSpanTime){
		boolean success = addMessage(msg);
		while(!success){
			try {
				Thread.sleep(waitSpanTime);
			} catch (InterruptedException e) {}
			success = addMessage(msg);
		}
	}

	/**
	 * 组件初始化,初始化完成即可处理消息
	 */
	public void init(){
		if(this.status > 1){
			log.warn("DisposeUnit status is invalid");
			return;
		}
		para.put("disposeUnitTag", disposeUnitTag);
		msgQueueList = new ArrayList<>();
		msgBrokerList = new ArrayList<>();
		for (int i = 0; i < handleThreadCount; i++) {
			//superviser
			DefaultSuperviser superviser = new DefaultSuperviser();
			superviser.init();
			//queue
			MessageQueue<StringMessage> queue = loadMessageQueue(messageQueueClazzName);
			queue.setSuperviser(superviser);
			if(queue instanceof JdbcMessageQueue) {
				JdbcMessageQueue jdbcQueue = (JdbcMessageQueue)queue;
				jdbcQueue.setDataSource(dataSource);
			}
			para.put("queueNo","queue"+i);
			queue.init(para);
			msgQueueList.add(queue);
			//broker
			SingleThreadMsgBroker broker = new SingleThreadMsgBroker();
			broker.setBrokerTag("broker-" + i);
			broker.setMessageQueue(queue);
			broker.setMessageHandler(messageHandler);
			broker.setSuperviser(superviser);
			broker.init();
			msgBrokerList.add(broker);
		}
		hashRouter = new ConsistencyHash(handleThreadCount);
		hashRouter.init();
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
		if(msgBrokerList!=null) {
			for (SingleThreadMsgBroker broker : msgBrokerList) {
				broker.stopWorking();
			}
		}
		log.info("DisposeUnit already stop");
	}
	/**
	 * 加载上一次组件停止后未处理的消息
	 * 或是执行失败消息再次执行
	 */
	public void loadPendingMessage(){
		log.info("DisposeUnit load pending message now");
		if(msgQueueList!=null) {
			for (MessageQueue<StringMessage> queue : msgQueueList) {
				queue.loadPendingMsgToQueue();
			}
		}
	}

	public Map<String,Object> getMonitorInfoSnapshot(){
		Map<String,Object> snap = new HashMap<>();
		snap.put("disposeUnitTag", disposeUnitTag);
		snap.put("status", status);
		snap.put("handleThreadCount", handleThreadCount);
		snap.put("messageQueueClazzName", messageQueueClazzName);
		List<Map<String,Object>> list = new ArrayList<>();
		for (SingleThreadMsgBroker broker : msgBrokerList) {
			list.add(broker.getMonitorInfoSnapshot());
		}
		snap.put("handleThreadList", list);
		return snap;
	}
	
	/**
	 * 如果添加消息失败,可使用此方法保存消息日志
	 * <br>添加失败抛出异常
	 */
	public void saveMsgLog(StringMessage msg) throws Exception {
		int index = generateRandom(msgQueueList.size());
		MessageQueue<StringMessage> queue = msgQueueList.get(index);
		queue.saveMsgLog(msg, 1);
	}
	
	private MessageQueue<StringMessage> loadMessageQueue(String msgQueueClazzName) {
		try {
			Class<?> clazz = getClass().getClassLoader().loadClass(msgQueueClazzName);
			@SuppressWarnings("unchecked")
			MessageQueue<StringMessage> queue = (MessageQueue<StringMessage>)clazz.newInstance();
			return queue;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public int getHandleThreadCount() {
		return handleThreadCount;
	}
	public void setHandleThreadCount(int handleThreadCount) {
		this.handleThreadCount = handleThreadCount;
	}
	public MessageHandler getMessageHandler() {
		return messageHandler;
	}
	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}
	public void setPara(Map<String, String> para) {
		this.para = para;
	}
	public void setDisposeUnitTag(String disposeUnitTag) {
		this.disposeUnitTag = disposeUnitTag;
	}
	public void setMessageQueueClazzName(String messageQueueClazzName) {
		this.messageQueueClazzName = messageQueueClazzName;
	}
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
}
