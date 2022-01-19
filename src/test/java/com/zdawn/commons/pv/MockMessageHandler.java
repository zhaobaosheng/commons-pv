package com.zdawn.commons.pv;

import java.util.Random;

public class MockMessageHandler implements MessageHandler {

	@Override
	public int handleMessage(StringMessage msg) throws Exception {
		int max = 90;
		int min =5;
		Random rand = new Random();
		int randNumber = rand.nextInt(max - min + 1) + min;
		System.out.println("randNumber="+randNumber+" ? 800");
		if(randNumber>800) throw new Exception("handle test exception");
//		Thread.sleep(randNumber);
		System.out.println("wait="+randNumber+"ms  "+msg.getPayload());
		return 1;
	}
}
