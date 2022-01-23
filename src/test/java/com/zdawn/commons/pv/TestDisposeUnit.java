package com.zdawn.commons.pv;

import java.util.HashMap;
import java.util.Map;

public class TestDisposeUnit {

	public static void main(String[] args) {
		Map<String,String> para = new HashMap<>();
		para.put("msgStorePath", "E:\\source-code\\mygit\\commons-pv\\src\\test\\pv");
		DisposeUnit disposeUnit = new DisposeUnit();
		disposeUnit.setMessageHandler(new MockMessageHandler());
		disposeUnit.setDisposeUnitTag("test");
		disposeUnit.setHandleThreadCount(10);
		disposeUnit.setMessageQueueClazzName("com.zdawn.commons.pv.FileMessageQueue");
		disposeUnit.setPara(para);
		//init
		disposeUnit.init();
		//add message
		int count = 0;
		while(true){
			count = count +1;
			try {
				StringMessage msg = new StringMessage("{\"result\":1,\"name\":\"java之父\"}");
				msg.setHashKey("==="+count);
				boolean success = disposeUnit.addMessage(msg);
				System.out.println("add message result="+success);
				if(count%100==0) disposeUnit.saveMsgLog(msg);
			} catch (Exception e) {}
			if(count%1000==0){
				System.out.println(disposeUnit.getMonitorInfoSnapshot());
				disposeUnit.loadPendingMessage();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
	}
}
