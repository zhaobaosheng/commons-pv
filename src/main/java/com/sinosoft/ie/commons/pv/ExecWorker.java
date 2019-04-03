package com.sinosoft.ie.commons.pv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecWorker implements Runnable {
	private static Logger log = LoggerFactory.getLogger(ExecWorker.class);
	private HandlingMediator mediator;
	private MessageWrapper msgWrapper;
	
	public ExecWorker(HandlingMediator mediator,MessageWrapper msgWrapper){
		this.mediator = mediator;
		this.msgWrapper = msgWrapper;
	}
	@Override
	public void run() {
		long start = System.currentTimeMillis();
		try {
			//invoke
			mediator.getMessageHandler().handleMessage(msgWrapper.getMsg());
			long end  = System.currentTimeMillis();
			//success
			mediator.informMessageHandleResult(start,end-start,true,msgWrapper);
		} catch (Exception e) {
			//error
			long end  = System.currentTimeMillis();
			mediator.informMessageHandleResult(start,end-start,false,msgWrapper);
			log.error("invoke MessageHandler.handleMessage",e);
		}
	}
}
