package com.sinosoft.ie.commons.pv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现消息队列接口，使用文件存储消息，保证消息不丢失
 * @author zhaobs
 * 2018-01-12
 */
public class FileMessageQueue implements MessageQueue{
	private static Logger logger = LoggerFactory.getLogger(FileMessageQueue.class);
	/**
	 * 队列最大深度
	 */
	private int maxSize = 200;
	/**
	 * task 队列
	 */
	private LinkedList<MessageWrapper> queueMsg = new LinkedList<MessageWrapper>();
	/**
	 * 存储消息的路径
	 */
	private String queueMessageStorePath;
	/**
	 * 正在处理消息路径
	 */
	private String pendingMsgPath="pending";
	/**
	 * 处理错误消息路径
	 */
	private String errorMsgPath="error";
	/**
	 * 消息处理次数
	 */
	private int msgHandleTimes = 3;
	/**
	 * message full class name 
	 */
	private String msgClazzName;
	
	private HandlingMediator mediator;
	/**
	 * 获取消息
	 */
	public synchronized MessageWrapper pollMessage(){
		try {
			while(queueMsg.size()==0){
				wait();
			}
		} catch (InterruptedException e) {
			logger.error("pollMessage",e);
		}
		return queueMsg.removeFirst();
	}
	/**
	 * 添加消息
	 */
	public synchronized boolean putMessage(Message msg){
		if(mediator.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize) return false;
		//保存消息到文件
		try {
			String fileName = saveMessage(msg,0,"que");
			MessageWrapper wrapper = new MessageWrapper(msg);
			wrapper.putAttribute("fileName", fileName);
			queueMsg.add(wrapper);
		} catch (Exception e) {
			logger.error("putMessage",e);
			return false;
		}
		notifyAll();
		return true;
	}
	private synchronized boolean putMessage(String fileName){
		if(mediator.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize) return false;
		try {
			int index = fileName.lastIndexOf('.');
			if(index==-1) return false;
			//rename
			String path = queueMessageStorePath+'/'+pendingMsgPath+'/'+fileName;
			String destFileName = fileName.substring(0,index)+".que";
			String destPath = queueMessageStorePath+'/'+pendingMsgPath+'/'+destFileName;
			if(!moveFile(path, destPath)) return false;
			//load data
			Class<?> clazz = getClass().getClassLoader().loadClass(msgClazzName);
			Message msg = (Message)clazz.newInstance();
			byte[] data = readFile(destPath);
			msg.importMessage(data);
			MessageWrapper wrapper = new MessageWrapper(msg);
			wrapper.putAttribute("fileName", destFileName);
			queueMsg.add(wrapper);
		} catch (Exception e) {
			logger.error("putMessage",e);
			return false;
		}
		notifyAll();
		return true;
	}
	/**
	 * 能否添加任务
	 */
	public boolean canAddMessage(){
		return queueMsg.size() < maxSize;
	}
	/**
	 * 获取当前任务队列深度
	 */
	public int getCurrentQueueSize(){
		return queueMsg.size();
	}

	public int getMaxSize() {
		return maxSize;
	}
	
	public void init() {
		if(mediator==null) throw new RuntimeException("HandlingMediator is not setting");
		//检查创建目录
		String path = queueMessageStorePath+'/'+pendingMsgPath;
		File file = new File(path);
		if(!file.exists()) file.mkdirs();
		path = queueMessageStorePath+'/'+errorMsgPath;
		file = new File(path);
		if(!file.exists()) file.mkdirs();
		//*.que消息重名*.msg
		path = queueMessageStorePath+'/'+pendingMsgPath;
		file = new File(path);
		File[] fileList = file.listFiles();
		for (File tempFile : fileList) {
			String fileName = tempFile.getName();
			if(!fileName.endsWith(".que")) continue;
			int index = fileName.lastIndexOf('.');
			String destFileName = fileName.substring(0,index)+".msg";
			String destPath = path+'/'+destFileName;
			if(!moveFile(tempFile.getAbsolutePath(),destPath)){
				logger.error(fileName+" rename "+destFileName+" error");
			}
		}
	}
	/**
	 * 保存消息
	 * 文件名规则 uuid-当前时间毫秒数-已处理次数
	 * @param msg Message
	 * @param times 执行次数
	 * @param ext msg 处理过的消息  que进入队列的消息
	 * @return 仅返回文件名
	 * @throws Exception
	 */
	private String saveMessage(Message msg,int times,String ext) throws Exception{
		String fileName = UUID.randomUUID().toString().replace("-", "")+'-'+System.currentTimeMillis()+'-'+times+'.'+ext;
		String path = queueMessageStorePath+'/'+pendingMsgPath+'/'+fileName;
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(path);
			fos.write(msg.exportMessage());
			fos.flush();
		} catch (Exception e) {
			throw e;
		}finally{
			try {if(fos!=null) fos.close();} catch (Exception e) {}
		}
		return fileName;
	}
	
	public void loadPendingMsgToQueue(){
		String path = queueMessageStorePath+'/'+pendingMsgPath;
		File file = new File(path);
		if (!file.exists()) {
			logger.error("load pending message to queue error "+path+" is not exist");
			return;
		}
		if (!file.isDirectory()) {
			logger.error("load pending message to queue error "+path+" is not directory");
			return;
		}
		ArrayList<String[]> list = new ArrayList<String[]>();
		String[] tempList = file.list();
		for (String temp : tempList) {
			if(!temp.endsWith(".msg")) continue;
			int index = temp.lastIndexOf('.');
			if(index==-1) continue;
			String nameExceptExt = temp.substring(0, index);
			String[] parts = nameExceptExt.split("-");
			if(parts.length!=3) continue;
			//check handle times
			if(check(parts[2], temp)) list.add(new String[]{parts[1],temp});
		}
		//按文件创建时间排序
		Collections.sort(list, new Comparator<String[]>(){
			@Override
			public int compare(String[] o1, String[] o2) {
				return o1[0].compareTo(o2[0]);
			}
		});
		//添加队列
		for (String[] temp : list) {
			if(!putMessage(temp[1])) break;
		}
	}
	
	public void onHandleMsgResult(boolean success, MessageWrapper msgWrapper) {
		Object obj = msgWrapper.getAttribute("fileName");
		if(obj==null || "".equals(obj)){
			logger.warn("the onHandleMsgResult method fileName is empty");
			return;
		}
		String fileName = obj.toString();
		if(success){
			removeMessage(fileName);
		}else{
			renameMsgFile(fileName, 1);
		}
	}
	
	public void configureMediator(HandlingMediator mediator) {
		this.mediator = mediator;
	}
	/**
	 * 检查消息执行次数
	 * 如果大于等于设置执行次数,移动到错误消息存储路径
	 */
	private boolean check(String times,String fileName){
		boolean add = true;
		try {
			int count = Integer.parseInt(times);
			if(count>=msgHandleTimes){
				add = false;
				String path = queueMessageStorePath+'/'+pendingMsgPath+'/'+fileName;
				String destPath = queueMessageStorePath +'/'+ errorMsgPath+'/'+fileName;
				if(!moveFile(path, destPath)){
					logger.error(fileName+" rename failture");
				}
			}
		} catch (NumberFormatException e) {
			logger.error(fileName+":file name format is not right");
		}
		return add;
	}
	/**
	 * 删除消息
	 */
	private void removeMessage(String fileName){
		String path = queueMessageStorePath+'/'+pendingMsgPath+'/'+fileName;
		File file = new File(path);
		if(!file.exists()) return;
		if(!file.delete()){//删除失败
			//记录日志
			saveErrorLog(file.getAbsolutePath());
		}
	}
	//记录删除失败日志
	private void saveErrorLog(String filePath){
		FileOutputStream fos = null;
		try {
			String path = queueMessageStorePath+'/'+pendingMsgPath+"/del-msg-error.log";
			fos = new FileOutputStream(path,true);
			fos.write((filePath+'\n').getBytes());
			fos.flush();
		} catch (Exception e) {
			logger.error("saveErrorLog",e);
		}finally{
			try {if(fos!=null) fos.close();} catch (Exception e) {}
		}
	}
	
	private void renameMsgFile(String fileName,int increaseTimes){
		String path = queueMessageStorePath+'/'+pendingMsgPath+'/'+fileName;
		int index = fileName.lastIndexOf('.');
		if(index==-1) return;
		String nameExceptExt = fileName.substring(0, index);
		String[] parts = nameExceptExt.split("-");
		if(parts.length!=3) return;
		int count = Integer.parseInt(parts[2]);
		count = count + increaseTimes;
		String destPath = "";
		if(count>=msgHandleTimes){//移动错误消息目录
			destPath = queueMessageStorePath+'/'+errorMsgPath+'/'+parts[0]+'-'+parts[1]+'-'+count+".msg";
		}else{//增加执行次数
			destPath = queueMessageStorePath+'/'+pendingMsgPath+'/'+parts[0]+'-'+parts[1]+'-'+count+".msg";
		}
		if(!moveFile(path, destPath)){
			logger.error(path+" rename "+destPath+" failture");
		}
	}
	private boolean copyFile(String originPath,String destPath){
		boolean result = true;
		FileInputStream fis = null;
        FileOutputStream fos = null;
        FileChannel inChannel = null;
        FileChannel outChannel = null;
        try {
        	fis = new FileInputStream(originPath);
            fos = new FileOutputStream(destPath);
            inChannel = fis.getChannel();
            outChannel = fos.getChannel();
            inChannel.transferTo(0, inChannel.size(),outChannel);
        } catch (IOException e) {
        	logger.error("copyFile",e);
        	result = false;
        } finally {
        	try {if(inChannel!=null) inChannel.close();} catch (IOException e){}
            try {if(fis!=null) fis.close();} catch (IOException e){}
            try {if(outChannel!=null) outChannel.close();} catch (IOException e){}
            try {if(fos!=null) fos.close();} catch (IOException e){}
        }
        return result;
	}
	
	private boolean moveFile(String originPath,String destPath){
		File ofile = new File(originPath);
		File dfile = new File(destPath);
		if(!ofile.renameTo(dfile)){//rename error
			boolean result = copyFile(originPath, destPath);
	    	if(!result) return false;
	    	if(!ofile.delete()){//delete destfile
	    		if(!dfile.delete()) logger.error("move file error,delete already copy file error path="+destPath);
	    		return false;
	    	}
		}
		return true;
	}
	
	private byte[] readFile(String filePath) throws Exception{  
        InputStream in = null;
        byte[] data = null;
        try {
            in = new FileInputStream(filePath);  
            data = new byte[in.available()];
            in.read(data);
        } catch (Exception e) {  
        	throw e;
        }finally{
        	try {if(in!=null) in.close();} catch (Exception e) {}
        }
        return data;
	}
	
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	public void setQueueMessageStorePath(String queueMessageStorePath) {
		this.queueMessageStorePath = queueMessageStorePath;
	}
	public void setMsgHandleTimes(int msgHandleTimes) {
		this.msgHandleTimes = msgHandleTimes;
	}
	public void setMsgClazzName(String msgClazzName) {
		this.msgClazzName = msgClazzName;
	}
}
