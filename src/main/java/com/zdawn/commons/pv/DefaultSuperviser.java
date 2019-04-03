package com.zdawn.commons.pv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSuperviser implements Superviser {
	private static Logger log = LoggerFactory.getLogger(DefaultSuperviser.class);
	/**
	 * 参与消息处理流程
	 */
	private boolean joinMessageProcessing = true;
	/**
	 * 统计滚动消息处理时长 单位是毫秒
	 */
    private long rollingStatsTime = 10000L;
    /**
     * 统计滚动消息处理时长分片数量
     */
    private int rollingStatsNumBuckets = 5;
    /**
     * 错误比率阀值 
     * <br>超出阀值暂停添加消息、处理消息
     */
    private float errorThresholdPercentage = 0.2f;
//    /**
//     * 最近 时间分片错误消息个数阀值
//     * <br>超出阀值暂停添加消息、处理消息
//     */
//    private long errorThresholdNumberOfLastBucket = 5;
    /**
     * 暂停消息处理流程时间
     */
    private long pauseMessageProcessingTime = 5000L;
    /**
     * 暂停消息处理开关
     * <br> 0 关闭
     * <br> 1 打开
     * <br> 2 半开
     */
    private int breaker = 0; 
    /**
     * 检查不处理消息的时间
     */
    private long curCheckMsgProcessingTime = 0L;
    /**
     * 消息处理接口实现类名
     */
    private String handleClazzName;
    /**
     * 统计消息处理结果最大缓存
     * <br>超过最大值舍弃
     */
    private int messageHandleResultMaxSize = 1000;
    /**
     * 消息处理统计链表
     */
    private LinkedList<StatBucktet> statsBucketList = new LinkedList<StatBucktet>();
    /**
     * 消息处理结果缓存
     */
    private LinkedList<Object[]> resultList = new LinkedList<Object[]>();
    /**
     * 是否统计消息处理结果
     */
    private boolean isStat = true;
    /**
     * 计算处理结果消息最大线程数
     */
    private int maxPoolSize = 5 ;
    private long keepAliveTime = 5000L;
    /**
	 * 等待分配消息处理结果再次执行的周期，单位毫秒
	 */
	private long waitExecutePeriod = 1000L;
    /**
	 * 线程池
	 */
	private ThreadPoolExecutor threadPool = null;
	/**
	 * 统计消息处理结果计算者
	 */
	private LinkedList<HandleResultCalculator> handleResultCalculatorList = new LinkedList<HandleResultCalculator>();
	/**
	 * 当前HandleResultCalculator数量
	 */
	private List<HandleResultCalculator> handleResultCalculatorAll = new ArrayList<HandleResultCalculator>();
    /**
     * 获取处理消息的开关
     */
    public int getBreakerStatus(){
    	if(!joinMessageProcessing) return 0;
    	if(breaker==1){//开关打开
    		if(System.currentTimeMillis()-curCheckMsgProcessingTime>pauseMessageProcessingTime){//大于阈值 将开关半开
    			breaker = 2 ;
    		}
    	}
    	return breaker;
    }
    
	public void forceCloseBreaker() {
		breaker = 0;
	}

	/**
     * 收集消息处理的结果
     * @param startHandlingTime 处理开始时间 毫秒
     * @param spendTime 花费时间 毫秒
     * @param success 消息处理是否成功
     */
    public void collectMessageHandleResult(long startHandlingTime,long spendTime,
			boolean success){
    	if(isStat){
    		Object[] result = new Object[]{success?1:0,startHandlingTime,spendTime};
    		if(resultList.size()<messageHandleResultMaxSize){
    			resultList.add(result);
    			synchronized (resultList) {
    				resultList.notify();
				}
    		}else{
    			log.warn("handle result message already reach max size "+messageHandleResultMaxSize+" clear old data");
    			synchronized (resultList) {
    				resultList.clear();
    				resultList.add(result);
				}
    		}
    	}
    }
    /**
     * 收集线程池拒绝数据
     * @param startHandlingTime 处理开始时间 毫秒 
     * @param spendTime 花费时间 毫秒
     */
    public void collectMessageRejection(long startHandlingTime,long spendTime){
    	if(isStat){
    		Object[] result = new Object[]{2,startHandlingTime,spendTime};
    		if(resultList.size()<messageHandleResultMaxSize){
    			resultList.add(result);
    			synchronized (resultList) {
    				resultList.notify();
				}
    		}else{
    			log.warn("handle result message already reach max size "+messageHandleResultMaxSize+" clear old data");
    			synchronized (resultList) {
    				resultList.clear();
    				resultList.add(result);
				}
    		}	
    	}
    }
    /**
     * 初始化
     */
    public void init(){
    	createThreadPool();
    	Thread thread = new Thread(new HandleResultBroker());
    	thread.start();
    }
    /**
     * 获取监控数据快照
     */
    public Map<String,Object> getMonitorInfoSnapshot(){
    	Map<String,Object> data = new HashMap<String,Object>();
    	data.put("handleClazzName", handleClazzName);
    	data.put("breaker", breaker);
    	data.put("handleMsgResultSize", resultList.size());
    	List<StatBucktet> list = new ArrayList<StatBucktet>();
    	for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
    		StatBucktet origin = it.next();
    		for (HandleResultCalculator handleResultCalculator : handleResultCalculatorAll) {
    			StatBucktet copy = handleResultCalculator.findCopyStatBucktet(origin);
    			if(copy!=null) origin.merge(copy);
			}
    		list.add(origin);
    	}
    	data.put("statsBucketList",list);
    	return data;
    }
    
    public void recycleHandleResultCalculator(HandleResultCalculator handleResultCalculator){
    	handleResultCalculatorList.push(handleResultCalculator);
    }
    /**
     * 根据处理时间找到StatBucktet
     * @param handlingTime 处理时间
     * @return null not found
     */
    public StatBucktet getStatBucktet(long handlingTime){
    	StatBucktet bucket = null;
    	for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
			StatBucktet temp = it.next();
    		if(temp.belongTimeBucktet(handlingTime)==0){
    			bucket = temp;
    			break;
    		}
    	}
    	return bucket;
    }
    /**
     * breaker can open
     */
    public void calcBreakerStatus(){
    	if(joinMessageProcessing && statsBucketList.size()>0){
    		long errorCount = 0L;
    		long successCount = 0L;
	    	for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
	    		StatBucktet origin = it.next();
	    		for (HandleResultCalculator handleResultCalculator : handleResultCalculatorAll) {
	    			StatBucktet copy = handleResultCalculator.findCopyStatBucktet(origin);
	    			if(copy!=null){
	    				errorCount = errorCount + copy.getFailureCount();
		    			successCount = successCount+copy.getSuccessCount();
	    			}
				}
	    	}
	    	float curPercentage = 1f*errorCount/(errorCount+successCount);
	    	if(curPercentage > errorThresholdPercentage){
	    		breaker = 1;
	    		curCheckMsgProcessingTime = System.currentTimeMillis();
	    	}	
    	}
    }
    
    private ThreadPoolExecutor createThreadPool(){
		if(threadPool!=null) return threadPool;
		threadPool = new ThreadPoolExecutor(1, maxPoolSize,
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
		}
		return success;
	}
    private HandleResultCalculator getHandleResultCalculator(){
    	if(handleResultCalculatorList.size()<1){
    		if(handleResultCalculatorAll.size() >= maxPoolSize){
    			return null;
    		}else{
    			HandleResultCalculator temp = new HandleResultCalculator(this);
    			handleResultCalculatorAll.add(temp);
    			handleResultCalculatorList.push(temp);
    		}
    	}
    	return handleResultCalculatorList.removeFirst();
    }
    /**
     * 维护消息处理结果统计链表
     */
    private void manageStatBucktetList(long handlingTime){
    	long cur = handlingTime>0 ? handlingTime:System.currentTimeMillis();
    	if(statsBucketList.size()>0){
    		StatBucktet bucket = statsBucketList.getFirst();
    		int value = bucket.belongTimeBucktet(cur);
    		if(value>0){
    			bucket = new StatBucktet(cur,rollingStatsTime/rollingStatsNumBuckets);
	    		statsBucketList.push(bucket);
	    		if(statsBucketList.size()>rollingStatsNumBuckets) statsBucketList.removeLast();
    		}
    	}else{
    		StatBucktet bucket = new StatBucktet(cur,rollingStatsTime/rollingStatsNumBuckets);
    		statsBucketList.push(bucket);
    	}
    }
    
    private void consumeMessageHandleResult(){
    	while(isStat){
    		try {
    			if(resultList.size()==0){
    				synchronized (resultList) {
    					resultList.wait();
					}
    			}
    			Object[] result = resultList.removeFirst();
    			manageStatBucktetList((Long)result[1]);
    			//执行
    			while(true){
    				HandleResultCalculator temp = getHandleResultCalculator();
    				if(temp!=null){
    					temp.setResult(result);
    					if(executeTask(temp,threadPool)) break;
    					else recycleHandleResultCalculator(temp);
    				}else{
						//等待一个周期
    					try {
    						log.warn("exec handle result threadpool is drain. Now wait next period to do!");
    						Thread.sleep(waitExecutePeriod);
    					} catch (InterruptedException e) {}	
    				}
    			}
    		} catch (InterruptedException e) {
    			log.error("consumeMessageHandleResult",e);
    		}
    	}
    }
    class HandleResultBroker implements Runnable{
		@Override
		public void run() {
			consumeMessageHandleResult();
		}
    }

	public void setHandleClazzName(String handleClazzName) {
		this.handleClazzName = handleClazzName;
	}
	
	public void stopStat(){
		this.isStat = true;
	}
	
	public void setJoinMessageProcessing(boolean joinMessageProcessing) {
		this.joinMessageProcessing = joinMessageProcessing;
	}
	public void setRollingStatsTime(long rollingStatsTime) {
		this.rollingStatsTime = rollingStatsTime;
	}
	public void setRollingStatsNumBuckets(int rollingStatsNumBuckets) {
		this.rollingStatsNumBuckets = rollingStatsNumBuckets;
	}
	public void setErrorThresholdPercentage(float errorThresholdPercentage) {
		this.errorThresholdPercentage = errorThresholdPercentage;
	}
	public void setPauseMessageProcessingTime(long pauseMessageProcessingTime) {
		this.pauseMessageProcessingTime = pauseMessageProcessingTime;
	}
	public void setMessageHandleResultMaxSize(int messageHandleResultMaxSize) {
		this.messageHandleResultMaxSize = messageHandleResultMaxSize;
	}
	public long getRollingStatsTime() {
		return rollingStatsTime;
	}
	public int getRollingStatsNumBuckets() {
		return rollingStatsNumBuckets;
	}
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}
	public void setWaitExecutePeriod(long waitExecutePeriod) {
		this.waitExecutePeriod = waitExecutePeriod;
	}
	public void setKeepAliveTime(long keepAliveTime) {
		this.keepAliveTime = keepAliveTime;
	}
}
