package com.sinosoft.ie.commons.pv;

public interface MsgBroker {
	/**
	 * the menthod will be invoke before working
	 */
	public void init();
	/**
	 * config HandlingMediator
	 * in order to communication with other class
	 * it should be set before init method  invoking
	 */
	public void configureMediator(HandlingMediator mediator);
	/**
	 * stop handle message
	 */
	public void stopWorking();
}
