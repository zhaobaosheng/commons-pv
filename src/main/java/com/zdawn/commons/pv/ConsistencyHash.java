package com.zdawn.commons.pv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * 一致性hash
 */
public class ConsistencyHash {
	//每个队列的虚拟节点个数
	private static final int V_NODE_NUM = 100;
	
	private int queueCount;
	/**
	 * 真实消息队列
	 * 假定给队列定义名字  队列索引
	 */
	private List<QueueNode> queueMap;
	
	private TreeMap<Long,QueueNode> nodes;
	
    public ConsistencyHash(int queueCount){
        this.queueCount = queueCount;
    }
    
    public void init(){
    	queueMap = new ArrayList<>();
    	nodes = new TreeMap<>();
    	for (int i = 0; i < queueCount; i++) {
    		QueueNode node = new QueueNode("queue-"+i, i);
    		queueMap.add(node);
    		nodes.put(hash(node.getName()), node);
    		for (int n = 0; n < V_NODE_NUM; n++) {
                Long key = hash(node.getName()+"-virtual-node-"+n);
                nodes.put(key,node);
            }
		}
    }
    
    public int selectHashQueueIndex(String key) {
    	Long hashCode = hash(key);
    	//沿环的顺时针找到一个虚拟节点
        SortedMap<Long, QueueNode> tail = nodes.tailMap(hashCode);
        QueueNode node = tail.size() == 0 ? nodes.get(nodes.firstKey()) : nodes.get(tail.firstKey());
        return node.getIndex();
    }

    /**
     *  MurMurHash算法，是非加密HASH算法，性能很高，
     *  比传统的CRC32,MD5，SHA-1
     *  （这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免）
     *  等HASH算法要快很多，而且据说这个算法的碰撞率很低.
     *  http://murmurhash.googlepages.com/
     */
    private Long hash(String key){

        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
            // for big-endian version, do this first:
            // finish.position(8-buf.remaining());
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }
}

class QueueNode{
    private String name;
    private Integer index;

    public QueueNode(String name, Integer index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	@Override
    public String toString() {
        return "QueueNode {" +"index='" + index + '\'' +", name='" + name + '\'' +'}';
    }
}
