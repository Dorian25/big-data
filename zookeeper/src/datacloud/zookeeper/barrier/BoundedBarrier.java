package datacloud.zookeeper.barrier;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class BoundedBarrier {
	
	private ZkClient zk_client;
	private String namebarrier;
	private int N;
	
	public BoundedBarrier(ZkClient zk_client, String namebarrier, int size) throws KeeperException, InterruptedException {
		this.N = size;
		this.namebarrier = namebarrier;
		this.zk_client = zk_client;
		
		if(this.zk_client.zk().exists(this.namebarrier, true) == null) {
			this.zk_client.zk().create(namebarrier,EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			for (int i = 0; i < this.N; i++) {
				this.zk_client.zk().create(this.namebarrier+"/"+i,EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		}
	}
	
	public void sync() throws InterruptedException, KeeperException {
		boolean exit = false;
		String zk_id = this.zk_client.id();
		int len_id = this.zk_client.id().length();
		String p_id = String.valueOf(zk_id.charAt(len_id-1));
		
		

		//#################################
		//#################################
		
		//1ERE VERSION => NE FONCTIONNE PAS
		//source/inspiration => DOUBLE BARRIERS => https://zookeeper.apache.org/doc/r3.6.2/recipes.html
		
		/*while(!exit) {
			this.zk_client.zk().sync(this.namebarrier, null, null);
			
			List<String> children = this.zk_client.zk().getChildren(this.namebarrier, false);
			System.out.println("p_id"+p_id+" children"+children);
			if(children.size() == 0) {
				synchronized(this.zk_client) {
					this.zk_client.notify();
				}
				exit=true;
				break;
			}
			if(children.size()== 1) {
				this.zk_client.zk().delete(this.namebarrier+"/"+children.get(0), -1);
				synchronized(this.zk_client) {
					this.zk_client.notify();
				}
				exit=true;
				break;
			}
			if(Integer.parseInt(children.get(0)) == Integer.parseInt(p_id)) {
				synchronized(this.zk_client) {
					this.zk_client.wait();
				}
			} else {
				if(this.zk_client.zk().exists(this.namebarrier+"/"+p_id, false) != null) {
					this.zk_client.zk().delete(this.namebarrier+"/"+p_id, -1);
					synchronized (this.zk_client) {
						this.zk_client.wait();	
					}
				}
			}
			
		}
		System.out.println(this.zk_client.zk().getChildren(this.namebarrier, false));
		this.zk_client.zk().delete(this.namebarrier, -1);

		this.zk_client.zk().sync(this.namebarrier, null, null);*/

		//#################################
		//#################################
		
		//2EME VERSION => NE FONCTIONNE PAS
		
		this.zk_client.zk().delete(this.namebarrier+"/"+p_id, -1);
		this.zk_client.zk().sync(this.namebarrier, null, null);
		
		
		while(!exit) {	
			this.zk_client.zk().sync(this.namebarrier, null, null);
			
			if(this.sizeBarrier() != 0) {
				synchronized (this.zk_client) {
					this.zk_client.wait();
				}
			} else {
				synchronized (this.zk_client) {
					this.zk_client.notify();
					exit = false;
				}
			}
		}
	}
	
	public int sizeBarrier() throws KeeperException, InterruptedException {
		this.zk_client.zk().sync(this.namebarrier, null, null);
		List<String> l = this.zk_client.zk().getChildren(this.namebarrier, false);
		return l.size();
	}
}
