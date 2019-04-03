package com.sinosoft.ie.commons.pv;

public class TestDisposeUnit {

	public static void main(String[] args) {
		DisposeUnit disposeUnit = new DisposeUnit();
		disposeUnit.setMessageHandler(new MockMessageHandler());
		DefaultMsgBroker msgBroker = new DefaultMsgBroker();
		msgBroker.setCorePoolSize(10);
		msgBroker.setMaxPoolSize(30);
		disposeUnit.setMsgBroker(msgBroker);
		FileMessageQueue msgQueue = new FileMessageQueue();
		msgQueue.setQueueMessageStorePath("C:/Users/zhaobaosheng/Desktop/tmp/pv-test");
		msgQueue.setMsgClazzName("com.sinosoft.ie.commons.pv.StringMessage");
		disposeUnit.setMsgQueue(msgQueue);
		DefaultSuperviser superviser = new DefaultSuperviser();
		superviser.setHandleClazzName("MockMessageHandler");
		disposeUnit.setSuperviser(superviser);
		//init
		disposeUnit.init();
		//add message
		int count = 0;
		while(true){
			count = count +1;
			try {
				StringMessage msg = new StringMessage("{\"result\":1,\"name\":\"java之父\"}");
				boolean success = disposeUnit.addMessage(msg);
				System.out.println("add message result="+success);
				Thread.sleep(500);
			} catch (Exception e) {}
			if(count%10==0){
				System.out.println(disposeUnit.getMonitorInfoSnapshot());
				disposeUnit.loadPendingMessage();
			}
		}
	}

}
