package com.zdawn.commons.pv;

import java.util.Map;

public interface MsgBroker {
	/**
	 * the menthod will be invoke before working
	 */
	public void init();
	/**
	 * stop handle message
	 */
	public void stopWorking();
	/**
	 * 获取监控信息快照
	 */
	public Map<String,Object> getMonitorInfoSnapshot();
}
