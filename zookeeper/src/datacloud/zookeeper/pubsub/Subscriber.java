package datacloud.zookeeper.pubsub;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class Subscriber extends ZkClient{

	private String name;
	private List<String> new_messages;
	
	public Subscriber(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		this.name = name;
		this.new_messages = new ArrayList<String>();
	}

	@Override
	public void process(WatchedEvent event) {
	}
	
	private List<String> getDataMessages(String t, List<String> path_m) throws KeeperException, InterruptedException {
		List<String> data_m = new ArrayList<String>();
		
		for (String p : path_m) {
			byte[] bytes = this.zk().getData("/subscribers/"+t+"/"+this.name+"/"+p, false, null);
			String s = new String(bytes, StandardCharsets.UTF_8);
			data_m.add(s);
		}
		return data_m;
	}
	
	public void subscribe(String topic) throws KeeperException, InterruptedException {
				
		if(this.zk().exists("/subscribers", false) == null) {
			this.zk().create("/subscribers", EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk().create("/subscribers/"+topic, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk().create("/subscribers/"+topic+"/"+this.name, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else if(this.zk().exists("/subscribers/"+topic, false) == null){
			this.zk().create("/subscribers/"+topic, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk().create("/subscribers/"+topic+"/"+this.name, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else if(this.zk().exists("/subscribers/"+topic+"/"+this.name, false) == null){
			this.zk().create("/subscribers/"+topic+"/"+this.name, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			System.out.println("Déjà sub !");
		}
		
		this.new_messages = this.zk().getChildren("/subscribers/"+topic+"/"+this.name, true);
	}
	
	public List<String> received(String topic) throws KeeperException, InterruptedException {
		if(this.zk().exists("/subscribers/"+topic+"/"+this.name, false) != null) {
			List<String> path_messages = this.zk().getChildren("/subscribers/"+topic+"/"+this.name, true);
			this.new_messages = getDataMessages(topic, path_messages);
			return this.new_messages;
		} else {
			return new ArrayList<String>();
		}
	}

}
