package com.zdawn.commons.pv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSuperviser implements Superviser {
	private static Logger log = LoggerFactory.getLogger(DefaultSuperviser.class);
	/**
	 * 断路器功能是否开启
	 */
	private boolean breakerOn = true;
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
    /**
     * 暂停消息处理时间
     */
    private long pauseMessageProcessingTime = 2000L;
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
     * 消息处理统计链表
     */
    private LinkedList<StatBucktet> statsBucketList = new LinkedList<StatBucktet>();

    /**
     * 获取处理消息的开关
     */
    public int getBreakerStatus(){
    	if(!breakerOn) return 0;
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
    	//断路器功能开启
    	if(breakerOn) {
    		calcHandleResultForBreaker(success, startHandlingTime, spendTime);
    	}
    }

	/**
     *  收集消息处理者拒绝执行结果
     * @param startHandlingTime 处理开始时间 毫秒 
     * @param spendTime 花费时间 毫秒
     */
    public void collectMessageRejection(long startHandlingTime,long spendTime){
    	//断路器功能开启
    	if(breakerOn) {
    		calcHandleResultForRejection(startHandlingTime, spendTime);
    	}
    }
 
    /**
     * 获取监控数据快照
     */
    public Map<String,Object> getMonitorInfoSnapshot(){
    	Map<String,Object> data = new HashMap<String,Object>();
    	data.put("breaker", breaker);
    	List<Map<String,Object>> list = new ArrayList<>();
    	for (StatBucktet statBucktet : statsBucketList) {
    		Map<String,Object> row = new HashMap<>();
    		row.put("successCount", statBucktet.getSuccessCount());
    		row.put("failureCount", statBucktet.getFailureCount());
    		row.put("rejectionCount", statBucktet.getRejectionCount());
    		row.put("successAvgHandleTime", statBucktet.getSuccessAvgHandleTime());
    		row.put("successMaxHandleTime", statBucktet.getSuccessMaxHandleTime());
    		row.put("statStartMsTime", statBucktet.getStatStartMsTime());
    		row.put("statTimeBucktet", statBucktet.getStatTimeBucktet());
    		list.add(row);
		}
    	data.put("statsBucketList",list);
    	return data;
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
    	if(breakerOn && statsBucketList.size()>0){
    		long errorCount = 0L;
    		long successCount = 0L;
	    	for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
	    		StatBucktet temp = it.next();
	    		errorCount = errorCount + temp.getFailureCount();
    			successCount = successCount+temp.getSuccessCount();
	    	}
	    	float curPercentage = 1f*errorCount/(errorCount+successCount);
	    	if(curPercentage > errorThresholdPercentage){
	    		breaker = 1;
	    		curCheckMsgProcessingTime = System.currentTimeMillis();
	    	}	
    	}
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
	
	public void init() {
	}
	
	private void calcHandleResultForBreaker(boolean success,long startHandlingTime,long spendTime) {
		//维护统计链表
		manageStatBucktetList(startHandlingTime);
		StatBucktet bucket = getStatBucktet(startHandlingTime);
    	if(bucket!=null){
    		bucket.calc(success ? 1:0, spendTime);
    		//处理失败情况-开关是否打开
        	if(!success) calcBreakerStatus();
    	}else {
    		log.error("origin StatBucktet is null,must be having bug.");
    	}
	}
	private void calcHandleResultForRejection(long startHandlingTime,long spendTime) {
		//维护统计链表
		manageStatBucktetList(startHandlingTime);
		StatBucktet bucket = getStatBucktet(startHandlingTime);
    	if(bucket!=null){
    		bucket.calc(2, spendTime);
    	}else {
    		log.error("origin StatBucktet is null,must be having bug.");
    	}
	}
	
	public void setBreakerOn(boolean breakerOn) {
		this.breakerOn = breakerOn;
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

	public long getPauseMessageProcessingTime() {
		return pauseMessageProcessingTime;
	}
	
}
