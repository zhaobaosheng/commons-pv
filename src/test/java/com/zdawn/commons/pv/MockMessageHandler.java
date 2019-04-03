package com.zdawn.commons.pv;

import java.util.Random;

import com.zdawn.commons.pv.Message;
import com.zdawn.commons.pv.MessageHandler;

public class MockMessageHandler implements MessageHandler {

	@Override
	public void handleMessage(Message msg) throws Exception {
		int max = 90;
		int min =5;
		Random rand = new Random();
		int randNumber = rand.nextInt(max - min + 1) + min;
		System.out.println("randNumber="+randNumber+" ? 800");
		if(randNumber>800) throw new Exception("handle test exception");
//		Thread.sleep(randNumber);
		System.out.println("wait="+randNumber+"ms  "+new String(msg.exportMessage(),"utf-8"));
	}
}
