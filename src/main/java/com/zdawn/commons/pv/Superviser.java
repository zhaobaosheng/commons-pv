package com.zdawn.commons.pv;

import java.util.Map;

public interface Superviser {
	/**
	 * 设置消息处理实现者的标识
	 */
	public void setHandleClazzName(String handleClazzName);
	/**
	 * 能否处理消息的开关
	 * <br> 0 关闭
     * <br> 1 打开
     * <br> 2 半开
	 */
	public int getBreakerStatus();
	/**
	 * 强制设置可处理消息
	 */
	public void forceCloseBreaker();
	/**
	 * 初始化
	 */
	public void init();
	/**
	 * 收集消息处理的结果
     * @param startHandlingTime 处理开始时间 毫秒
     * @param spendTime 花费时间 毫秒
     * @param success 消息处理是否成功
	 */
	public void collectMessageHandleResult(long startHandlingTime,long spendTime,boolean success);
	/**
	 * 收集拒绝执行消息处理数据
	 * @param startHandlingTime 处理开始时间 毫秒 
	 * @param spendTime 花费时间 毫秒
	 */
    public void collectMessageRejection(long startHandlingTime,long spendTime);
	/**
	 * 获取监控信息快照
	 */
	public Map<String,Object> getMonitorInfoSnapshot();
}
