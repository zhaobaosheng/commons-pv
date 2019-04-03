package com.sinosoft.ie.commons.pv;

public class StatBucktet {
	/**
	 * 成功次数
	 */
	private long successCount = 0L;
	/**
	 * 失败次数
	 */
	private long failureCount = 0L;
	/**
	 * 线程池拒绝执行次数
	 */
	private long rejectionCount = 0L;
	/**
	 * 成功平均耗时
	 */
	private float successAvgHandleTime = 0f;
	/**
	 * 成功最大耗时
	 */
	private long successMaxHandleTime = 0L;
	/**
	 * 统计开始时间
	 */
	private long statStartMsTime;
	/**
	 * 统计时间段
	 */
	private long statTimeBucktet;
	
	public StatBucktet(long statStartMsTime,long statTimeBucktet){
		this.statStartMsTime = statStartMsTime;
		this.statTimeBucktet=statTimeBucktet;
	}
	/**
	 * 计算是否属于这个时间段
	 * 0属于
	 * 1大于
	 * -1小于
	 */
	public int belongTimeBucktet(long time){
		if(time < statStartMsTime) return -1;
		if(time > statStartMsTime+statTimeBucktet) return 1;
		return 0;
	}
	/**
	 * 计算一个消息处理结果
	 * @param type 0 失败 1成功 2 线程池拒绝
	 * @param spendTime
	 */
	public void calc(int type,long spendTime){
		if(type==0){
			failureCount = failureCount + 1;
		}else if(type==1){
			successAvgHandleTime = (successCount*successAvgHandleTime+spendTime)/(successCount+1);
			successCount = successCount +1;
			if(spendTime > successMaxHandleTime) successMaxHandleTime = spendTime;
		}else if(type==2){
			rejectionCount = rejectionCount +1;
		}
	}
	public void merge(StatBucktet copy){
		if(successCount+copy.successCount>0){
			successAvgHandleTime = (successCount*successAvgHandleTime+copy.successAvgHandleTime*copy.successCount)/(successCount+copy.successCount);
		}
		failureCount = failureCount + copy.failureCount;
		rejectionCount = rejectionCount + copy.rejectionCount;
		successCount = successCount + copy.successCount;
		if(copy.successMaxHandleTime>successMaxHandleTime) successMaxHandleTime = copy.successMaxHandleTime;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("successCount="+successCount).append('\n');
		sb.append("failureCount="+failureCount).append('\n');
		sb.append("rejectionCount="+rejectionCount).append('\n');
		sb.append("successAvgHandleTime="+successAvgHandleTime).append('\n');
		sb.append("successMaxHandleTime="+successMaxHandleTime).append('\n');
		sb.append("statStartMsTime="+statStartMsTime).append(" statTimeBucktet="+statTimeBucktet).append('\n');
		return sb.toString();
	}
	
	public long getSuccessCount() {
		return successCount;
	}

	public void setSuccessCount(long successCount) {
		this.successCount = successCount;
	}

	public long getFailureCount() {
		return failureCount;
	}

	public void setFailureCount(long failureCount) {
		this.failureCount = failureCount;
	}

	public long getRejectionCount() {
		return rejectionCount;
	}

	public void setRejectionCount(long rejectionCount) {
		this.rejectionCount = rejectionCount;
	}

	public float getSuccessAvgHandleTime() {
		return successAvgHandleTime;
	}

	public void setSuccessAvgHandleTime(float successAvgHandleTime) {
		this.successAvgHandleTime = successAvgHandleTime;
	}

	public long getSuccessMaxHandleTime() {
		return successMaxHandleTime;
	}

	public void setSuccessMaxHandleTime(long successMaxHandleTime) {
		this.successMaxHandleTime = successMaxHandleTime;
	}
	public long getStatStartMsTime() {
		return statStartMsTime;
	}
	public long getStatTimeBucktet() {
		return statTimeBucktet;
	}
	
}
