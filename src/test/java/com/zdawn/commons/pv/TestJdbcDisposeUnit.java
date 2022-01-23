package com.zdawn.commons.pv;

import java.util.HashMap;
import java.util.Map;

public class TestJdbcDisposeUnit {

	public static void main(String[] args) {
		//data source
//		DruidDataSource datasource = new DruidDataSource();
//        datasource.setUrl("jdbc:mysql://localhost:3306/edi_adr?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=GMT%2B8&useSSL=false");
//        datasource.setUsername("root");
//        datasource.setPassword("sinosoft-123456");
//        datasource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//        datasource.setInitialSize(2);
//        datasource.setMinIdle(2);
//        datasource.setMaxActive(10);
//        datasource.setMaxWait(60000);
//        datasource.setTimeBetweenEvictionRunsMillis(60000);
//        datasource.setMinEvictableIdleTimeMillis(300000);
//        datasource.setTestWhileIdle(true);
//        datasource.setTestOnBorrow(false);
//        datasource.setTestOnReturn(false);
//        datasource.setPoolPreparedStatements(true);
//        datasource.setMaxOpenPreparedStatements(20);
//        datasource.setAsyncInit(true);
        
		Map<String,String> para = new HashMap<>();
		para.put("msgStoreTableName", "msg_queue_store");
		para.put("msgLogTableName", "msg_queue_log");
		para.put("queueMaxSize", "201");
		para.put("msgHandleTimes", "3");
		
		DisposeUnit disposeUnit = new DisposeUnit();
		disposeUnit.setMessageHandler(new MockMessageHandler());
		disposeUnit.setDisposeUnitTag("test");
		disposeUnit.setHandleThreadCount(10);
		disposeUnit.setMessageQueueClazzName("com.zdawn.commons.pv.JdbcMessageQueue");
		disposeUnit.setPara(para);
//		disposeUnit.setDataSource(datasource);
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
				if(count%10==0) disposeUnit.saveMsgLog(msg);
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
