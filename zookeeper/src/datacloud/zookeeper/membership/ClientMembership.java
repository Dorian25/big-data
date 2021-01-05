package datacloud.zookeeper.membership;

import java.util.List;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import datacloud.zookeeper.ZkClient;

public class ClientMembership extends ZkClient {
	
	private List<String> zkclients_connect;
	
	public ClientMembership(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		this.zkclients_connect = this.zk().getChildren("/ids", true);
	}
	
	public List<String> getMembers() {
		try {
			this.zk().getChildren("/ids", true);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this.zkclients_connect;
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(event.getType() == EventType.NodeChildrenChanged) {
			try {
				this.zkclients_connect = this.zk().getChildren("/ids", true);
				this.zk().sync("/ids", null, null);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
