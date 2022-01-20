package com.zdawn.commons.pv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcMessageQueue implements MessageQueue<StringMessage> {
	private static Logger logger = LoggerFactory.getLogger(JdbcMessageQueue.class);
	/**
	 * 队列最大深度
	 */
	private int maxSize = 200;
	/**
	 * message 队列
	 */
	private LinkedList<MessageWrapper<StringMessage>> queueMsg = new LinkedList<>();
	/**
	 * 存储消息表名
	 */
	private String msgStoreTableName;
	/**
	 * 消息日志表名
	 */
	private String msgLogTableName;
	/**
	 * 处理单元标识
	 */
	private String disposeUnitTag;
	/**
	 * 队列编号
	 */
	private String queueNo;
	/**
	 * 存储消息数据库源
	 */
	private DataSource dataSource; 
	/**
	 * 消息处理次数
	 */
	private int msgHandleTimes = 3;
	/**
	 * 监控类
	 */
	private Superviser superviser;
	/**
	 * 获取消息
	 */
	public synchronized MessageWrapper<StringMessage> pollMessage(){
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
	public synchronized boolean putMessage(StringMessage msg) {
		if(superviser.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize) return false;
		//保存消息到数据库
		try {
			saveMsg(msg);
			MessageWrapper<StringMessage> wrapper = new MessageWrapper<StringMessage>(msg);
			queueMsg.add(wrapper);
		} catch (Exception e) {
			logger.error("putMessage",e);
			return false;
		}
		notify();
		return true;
	}
	
	private void saveMsg(StringMessage msg) throws Exception {
		String sql = "insert into "+msgStoreTableName+"(id,queue_no,hash_key,content,create_time,exec_count,msg_state)"
				+ " values(?,?,?,?,?,?,?,?)";
		Connection con = null;
		PreparedStatement ps = null;
		if(msg.getMessageId()==null) {
			msg.setMessageId(UUID.randomUUID().toString());
		}
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement(sql);
			ps.setString(1,msg.getMessageId());
			ps.setString(2,queueNo);
			ps.setString(3,msg.getHashKey());
			ps.setString(4,msg.getPayload());
			ps.setTimestamp(5,new Timestamp(System.currentTimeMillis()));
			ps.setInt(6,0);
			ps.setInt(7,1);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("saveMsg", e);
			throw e;
		} finally {
			closeStatement(ps);
			closeConnection(con);
		}
	}

	private void removeMsg(String msgId){
		String sql = "delete from "+msgStoreTableName+" where id=?";
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement(sql);
			ps.setString(1,msgId);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("msgId="+msgId+" delete error!");
			logger.error("removeMsg", e);
		} finally {
			closeStatement(ps);
			closeConnection(con);
		}
	}
	private void updateMsgExecFailure(String msgId){
		String selectSql = "select exec_count,msg_state from "+msgStoreTableName+" where id=?";
		String updateSql = "update "+msgStoreTableName+" set msg_state=?,exec_count=?,exec_finishtime=? where id=?";
		Connection con = null;
		PreparedStatement ps = null;
		PreparedStatement psUpdate = null;
		ResultSet rs = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement(selectSql);
			ps.setString(1,msgId);
			rs=ps.executeQuery();
			if(!rs.next()) throw new Exception("msgId="+msgId+ " is not exist");
			int execCount = rs.getInt(1);
			int msgState = rs.getInt(2);
			if(msgState!=1){
				logger.warn("message state is not correct. msgState="+msgState);
			}
			execCount = execCount +1;
			if(execCount>=msgHandleTimes){
				msgState = 3;
			}else{
				msgState = 2;
			}
			psUpdate = con.prepareStatement(updateSql);
			psUpdate.setInt(1,msgState);
			psUpdate.setInt(2,execCount);
			psUpdate.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
			psUpdate.setString(4,msgId);
			psUpdate.executeUpdate();
		} catch (Exception e) {
			logger.error("updateMsgExecFailure", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(psUpdate);
			closeConnection(con);
		}
	}
	
	private void loadMsgToQueue(String msgId,Connection con){
		String selectSql = "select exec_count,msg_state,content,hash_key from "+msgStoreTableName+" where id=?";
		String updateSql = null;
		PreparedStatement ps = null;
		PreparedStatement psUpdate = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(selectSql);
			ps.setString(1,msgId);
			rs=ps.executeQuery();
			if(!rs.next()) throw new Exception("msgId="+msgId+ " is not exist");
			int execCount = rs.getInt(1);
			int msgState = rs.getInt(2);
			if(msgState!=0 && msgState!=2) throw new Exception("msgId="+msgId+ " message state is not correct msgState="+msgState);
			String payload = rs.getString(3);
			if(payload==null) throw new Exception("msgId="+msgId+ " message is empty");
			String hashKey = rs.getString(4);
			//执行次数大于设置值
			if(execCount>=msgHandleTimes){
				//状态转为错误消息
				updateSql = "update "+msgStoreTableName+" set msg_state=? where id=?";
				psUpdate = con.prepareStatement(updateSql);
				psUpdate.setInt(1,3);
				psUpdate.setString(2,msgId);
				psUpdate.executeUpdate();
			}else{//添加队列
				StringMessage msg = new StringMessage(payload, hashKey);
				msg.setMessageId(msgId);
				if(putMessage(msg)){
					updateSql = "update "+msgStoreTableName+" set msg_state=? where id=?";
					psUpdate = con.prepareStatement(updateSql);
					psUpdate.setInt(1,1);
					psUpdate.setString(2,msgId);
					psUpdate.executeUpdate();
				}
			}
		} catch (Exception e) {
			logger.error("loadMsgToQueue", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(psUpdate);
		}
	}
	
	private void updateMsgNonexecution() throws Exception {
		String sql="update "+msgStoreTableName+" set msg_state=0 where msg_state=1 and queue_no=?";
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement(sql);
			ps.setString(1, queueNo);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("updateMsgNonexecution", e);
			throw e;
		} finally {
			closeStatement(ps);
			closeConnection(con);
		}
	}
	
	public void saveMsgLog(StringMessage msg,int msgSource) throws Exception {
		String selectSql = "select id from "+msgLogTableName+" where id=?";
		String insertSql = "insert into "+msgLogTableName+"(id,hash_key,content,create_time,dispose_unit_tag,msg_source) values (?,?,?,?,?,?)";
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		PreparedStatement psInsert = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement(selectSql);
			ps.setString(1, msg.getMessageId());
			rs = ps.executeQuery();
			boolean exist = rs.next();
			if(!exist) {
				psInsert = con.prepareStatement(insertSql);
				ps.setString(1,msg.getMessageId());
				ps.setString(2,msg.getHashKey());
				ps.setString(3,msg.getPayload());
				ps.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
				ps.setString(5,disposeUnitTag);
				ps.setInt(6,msgSource);
				ps.executeUpdate();
			}else {
				logger.warn("message already exist id="+msg.getMessageId());
			}
		} catch (Exception e) {
			logger.error("saveMsg", e);
			throw e;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(psInsert);
			closeConnection(con);
		}
	}
	
	private List<String> loadPendingMsgId(Connection conn){
		List<String> list = new ArrayList<String>();
		String sql = "select id from "+msgStoreTableName+" where msg_state in(0,2) and queue_no=? order by create_time";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, queueNo);
			rs = ps.executeQuery();
			while(rs.next()){
				list.add(rs.getString(1));
				if(list.size() > maxSize) break;
			}
		} catch (Exception e) {
			logger.error("loadPendingMsgId", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
		}
		return list;
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
	
	public void init(Map<String, String> para) {
		//初始化参数
		String temp = para.get("queueMaxSize");
		if(temp!=null && !"".equals(temp)) {
			maxSize = Integer.parseInt(temp);
		}
		temp = para.get("msgHandleTimes");
		if(temp!=null && !"".equals(temp)) {
			msgHandleTimes = Integer.parseInt(temp);
		}
		temp = para.get("msgStoreTableName");
		if(temp ==null || "".equals(temp)) throw new RuntimeException("msgStoreTableName 未设置");
		msgStoreTableName = temp;
		
		temp = para.get("queueNo");
		if(temp ==null || "".equals(temp)) throw new RuntimeException("queueNo 未设置");
		queueNo = temp;
		
		temp = para.get("msgLogTableName");
		if(temp ==null || "".equals(temp)) throw new RuntimeException("msgLogTableName 未设置");
		msgLogTableName = temp;
		
		temp = para.get("disposeUnitTag");
		if(temp ==null || "".equals(temp)) throw new RuntimeException("disposeUnitTag 未设置");
		disposeUnitTag = temp;
		
		if(dataSource==null) throw new RuntimeException("数据源未设置 在DisposeUnit类中设置");
		//消息状态为执行中消息更新为未执行
		try {
			updateMsgNonexecution();
		} catch (Exception e) {}
	}
	
	public void loadPendingMsgToQueue(){
		Connection con = null;
		try {
			con = dataSource.getConnection();
			//load pending message id
			List<String> list = loadPendingMsgId(con);
			for (String id : list) {
				loadMsgToQueue(id, con);
			}
		} catch (Exception e) {
			logger.error("loadPendingMsgToQueue", e);
		} finally {
			closeConnection(con);
		}
	}
	
	public void onHandleMsgResult(int result, MessageWrapper<StringMessage> msgWrapper) {
		StringMessage msg = msgWrapper.getMsg();
		if(result==1){
			removeMsg(msg.getMessageId());
		}else if(result==0){
			updateMsgExecFailure(msg.getMessageId());
		}else {
			try {
				saveMsgLog(msg, 2);
				removeMsg(msg.getMessageId());
			} catch (Exception e) {
				logger.error("saveMsgLog", e);
			}
		}
	}
	
	public void setSuperviser(Superviser superviser) {
		this.superviser = superviser;
	}
	
	private void closeStatement(Statement stmt){
		try {
			if(stmt!=null) stmt.close();
		} catch (SQLException e) {
			logger.error("closeStatement",e);
		}
	}
	private void closeResultSet(ResultSet rs){
		try {
			if(rs!=null) rs.close();
		} catch (SQLException e) {
			logger.error("closeSResultSet",e);
		}
	}
	private void closeConnection(Connection connection){
		try {
			if(connection !=null) connection.close();
		} catch (Exception e) {
			logger.error("closeConnection",e);
		}
	}
	

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	public void setMsgStoreTableName(String msgStoreTableName) {
		this.msgStoreTableName = msgStoreTableName;
	}
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	public void setMsgHandleTimes(int msgHandleTimes) {
		this.msgHandleTimes = msgHandleTimes;
	}
}
