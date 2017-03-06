package com.zookeeper1;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

/**
 * zookeeper实例2，选举leader
 * 
 * 你可以在takeLeadership进行任务的分配等等，并且不要返回，如果你想要要此实例一直是leader的话可以加一个死循环。
leaderSelector.autoRequeue();保证在此实例释放领导权之后还可能获得领导权。
在这里我们使用AtomicInteger来记录此client获得领导权的次数， 它是"fair"， 每个client有平等的机会获得领导权。
 * @author dell
 *
 */
public class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {
	private final String name;
	private final LeaderSelector leaderSelector;
	private final AtomicInteger leaderCount = new AtomicInteger();
	public ExampleClient(CuratorFramework client, String path, String name) {
		this.name = name;
		leaderSelector = new LeaderSelector(client, path, this);
		//leaderSelector.autoRequeue();保证在此实例释放领导权之后还可能获得领导权。
		leaderSelector.autoRequeue();
	}
	
	//启动
	public void start() throws IOException {
		leaderSelector.start();
	}
	
	//关闭
	public void close() throws IOException{
		leaderSelector.close();
	}
	
	public void takeLeadership(CuratorFramework client) throws Exception {
		final int waitSeconds = (int) (5 * Math.random()) + 1;
		/*客户机# 6现在是领袖。等待3秒……
		客户机# 6已经领袖0时间(s)。
		客户机# 6放弃领导。*/
		System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
		System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
		try {
			Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
		} catch (InterruptedException e) {
			System.err.println(name + " was interrupted.");
			Thread.currentThread().interrupt();
		} finally {
			System.out.println(name + " relinquishing leadership.\n");
		}
	}
}