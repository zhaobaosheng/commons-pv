package com.sinosoft.ie.commons.pv;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费消息线程
 * @author zhaobs
 * 2018-01-15
 */
public class DefaultMsgBroker extends Thread implements MsgBroker{
	private static Logger log = LoggerFactory.getLogger(DefaultMsgBroker.class);
	/**
	 * start or stop
	 */
	private boolean startBroker = true;
	/**
	 * 中介
	 */
	private HandlingMediator mediator;
	/**
	 * if threads idle keep threads size
	 */
	private int corePoolSize = 1;
	/**
	 * 处理任务线程最大数
	 */
	private int maxPoolSize = 5 ;
	private long keepAliveTime = 5000L;
	/**
	 * 线程池
	 */
	private ThreadPoolExecutor threadPool = null;
	/**
	 * 等待再次执行任务时间，单位毫秒
	 */
	private long waitExecutePeriod = 3000L;
	/**
	 * 等待消息处理结束时间 ,单位毫秒,默认10秒
	 */
	private long waitMessageExecTerminateTime=10000L;
	
	public void configureMediator(HandlingMediator mediator) {
		this.mediator = mediator;
	}

	@Override
	public void stopWorking() {
		try {
			startBroker = false;
			//stop thread pool
			if(threadPool!=null){
				threadPool.shutdown();
				threadPool.awaitTermination(waitMessageExecTerminateTime, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
			log.error("stopWorking",e);
		}
	}
	
	public void run() {
		while(startBroker)
		{
			MessageWrapper msgWrapper = mediator.pollMessage();
			if(msgWrapper!=null){
				try {
					handleMessage(msgWrapper);
				} catch (Exception e) {
					log.error("invoke handleMessage error",e);
				}
			}
		}
	}
	/**
	 * 初始化方法
	 */
	public void init(){
		if(threadPool==null) createThreadPool();
		if(mediator==null) throw new RuntimeException("HandlingMediator is not setting");
		this.start();
	}
	/**
	 * 从队列取出任务,使用线程池执行
	 * <br>如果线程池没有空闲线程等待waitExecutePeriod时间间隔，再次执行。
	 */
	public void handleMessage(MessageWrapper msgWrapper) {
		ExecWorker eworker = new ExecWorker(mediator, msgWrapper);
		//如果不能放到线程池执行,阻塞当前线程，直到能执行为止
		while(true){
			int status = mediator.getBreakerStatus();
			if(status==0){//close
				if(executeTask(eworker,threadPool)) break;
				//等待一个周期
				try {
					log.warn("the threadpool is drain. Now wait next period to do!");
					Thread.sleep(waitExecutePeriod);
				} catch (InterruptedException e) {}				
			}else if(status==1){//open
				try {
					log.warn("breaker status is open. Now wait next period to do!");
					Thread.sleep(waitExecutePeriod);
				} catch (InterruptedException e) {}
			}else{//semi-open
				long start = System.currentTimeMillis();
				try {
					//invoke
					mediator.getMessageHandler().handleMessage(msgWrapper.getMsg());
					long end  = System.currentTimeMillis();
					//success
					mediator.informMessageHandleResult(start,end-start,true,msgWrapper);
					mediator.forceCloseBreaker();
				} catch (Exception e) {
					//error
					long end  = System.currentTimeMillis();
					mediator.informMessageHandleResult(start,end-start,false,msgWrapper);
					log.error("invoke MessageHandler.handleMessage",e);
				}
				break;
			}
		}
	}
	private ThreadPoolExecutor createThreadPool(){
		if(threadPool!=null) return threadPool;
		threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
				keepAliveTime, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>());
		return threadPool;
	}
	private boolean executeTask(Runnable eworker,ThreadPoolExecutor threadPool){
		boolean success = true;
		try {
			threadPool.execute(eworker);
		} catch (RejectedExecutionException e) {
			success = false;
			mediator.informMessageRejection(System.currentTimeMillis(),0L);
		}
		return success;
	}
	
	public int getCorePoolSize() {
		return corePoolSize;
	}
	
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}
	
	public int getMaxPoolSize() {
		return maxPoolSize;
	}
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public void setWaitExecutePeriod(long waitExecutePeriod) {
		this.waitExecutePeriod = waitExecutePeriod;
	}

	public void setWaitMessageExecTerminateTime(long waitMessageExecTerminateTime) {
		this.waitMessageExecTerminateTime = waitMessageExecTerminateTime;
	}
}
