package com.zdawn.commons.pv;

import java.util.Random;

public class MockMessageHandler implements MessageHandler {

	@Override
	public int handleMessage(StringMessage msg) throws Exception {
		int max = 500;
		int min =5;
		Random rand = new Random();
		int randNumber = rand.nextInt(max - min + 1) + min;
		System.out.println("randNumber="+randNumber+" ? 400");
		if(randNumber>400) throw new Exception("handle test exception");
//		Thread.sleep(randNumber);
		System.out.println("wait="+randNumber+"ms  "+msg.getPayload()+" hashkey "+msg.getHashKey());
		return randNumber<100 ? 2:1;
	}
}
