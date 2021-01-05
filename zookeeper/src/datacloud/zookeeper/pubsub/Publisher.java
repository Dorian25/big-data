package datacloud.zookeeper.pubsub;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class Publisher extends ZkClient{
	private String name;
	
	public Publisher(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		this.name = name;
	}

	@Override
	public void process(WatchedEvent arg0) {
	}
	
	public void publish(String topic, String message) throws KeeperException, InterruptedException {
		System.out.println(this.name+" want to publish in "+topic+" this message :"+message);
		
		if(this.zk().exists("/subscribers", false) == null) {
			System.out.println("Aucun subscriber, donc pas besoin d'envoyer");
		} else {			
			if(this.zk().exists("/subscribers/"+topic, false) == null) {
				System.out.println("Aucun subscriber, donc pas besoin d'envoyer");
			} else {
				List<String> all_subscribers = this.zk().getChildren("/subscribers/"+topic, false);
				
				for (String sb : all_subscribers) {
					String id_message = UUID.randomUUID().toString();
					this.zk().create("/subscribers/"+topic+"/"+sb+"/"+id_message, message.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
				}
			}
		}
	}
}
