package com.zdawn.commons.pv;

import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 统计消息处理结果
 * @author zhaobaosheng
 */
public class HandleResultCalculator implements Runnable{
	private static Logger log = LoggerFactory.getLogger(HandleResultCalculator.class);
	/**
	 * 处理结果数据
	 */
	private Object[] result;
	/**
     * 消息处理统计链表
     */
	private LinkedList<StatBucktet> statsBucketList = new LinkedList<StatBucktet>();
	
	private DefaultSuperviser defaultSuperviser;
	
	public HandleResultCalculator(DefaultSuperviser defaultSuperviser){
		this.defaultSuperviser = defaultSuperviser;
	}
	
	public void setResult(Object[] result) {
		this.result = result;
	}

	@Override
	public void run() {
		StatBucktet bucket = null;
    	if(statsBucketList.size()>0){
    		bucket = statsBucketList.getFirst();
    		int value = bucket.belongTimeBucktet((Long)result[1]);
    		if(value>0){//需要添加
    			StatBucktet origin = defaultSuperviser.getStatBucktet((Long)result[1]);
    			if(origin!=null){
    				bucket = new StatBucktet(origin.getStatStartMsTime(),origin.getStatTimeBucktet());
    				statsBucketList.push(bucket);
    	    		if(statsBucketList.size()>defaultSuperviser.getRollingStatsNumBuckets()) statsBucketList.removeLast();
    			}else{
    				log.error("origin StatBucktet is null,must be having bug.");
    			}
    		}else if(value<0){
    			for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
    				StatBucktet temp = it.next();
    	    		if(temp.belongTimeBucktet((Long)result[1])==0){
    	    			bucket = temp;
    	    			break;
    	    		}
    	    	}
    		}
    	}else{//statsBucketList==0
    		StatBucktet origin = defaultSuperviser.getStatBucktet((Long)result[1]);
    		if(origin!=null){
				bucket = new StatBucktet(origin.getStatStartMsTime(),origin.getStatTimeBucktet());
				statsBucketList.push(bucket);
			}else{
				log.error("origin StatBucktet is null,must be having bug.");
			}
    	}
    	if(bucket!=null){
    		bucket.calc((Integer)result[0], (Long)result[2]);
    	}
    	//处理失败情况-开关是否打开
    	if(result[0].equals(0)) defaultSuperviser.calcBreakerStatus();
    	//release HandleResultCalculator
    	defaultSuperviser.recycleHandleResultCalculator(this);
	}
	
	public StatBucktet findCopyStatBucktet(StatBucktet origin){
		for(Iterator<StatBucktet> it = statsBucketList.iterator(); it.hasNext();){
			StatBucktet temp = it.next();
    		if(temp.getStatStartMsTime()==origin.getStatStartMsTime()
    				&& temp.getStatTimeBucktet()==origin.getStatTimeBucktet()){
    			return temp;
    		}
    	}
		return null;
	}
}