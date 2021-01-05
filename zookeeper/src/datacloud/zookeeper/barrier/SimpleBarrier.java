package datacloud.zookeeper.barrier;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class SimpleBarrier {
	
	private ZkClient zk_client;
	private String barriere;
	
	public SimpleBarrier(ZkClient zk_client, String path) throws KeeperException, InterruptedException {
		// si barriere n'existe pas, création du znode barriere
		if(zk_client.zk().exists(path, false) == null) {
			zk_client.zk().create(path, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		this.zk_client = zk_client;
		this.barriere = path;			
	}

	public void sync() throws KeeperException, InterruptedException {		
		boolean cond = false;
		while(!cond) {
			// synchronisation du znode barriere
			this.zk_client.zk().sync(this.barriere, null, null);
			// si barriere n'existe pas, on réveil le thread du zkclient
			if(this.zk_client.zk().exists(this.barriere, false) == null) {
				synchronized (this.zk_client) {
					this.zk_client.notify();
					cond=true;
				}
			// sinon on le fait attendre jusqu'à ce que la barriere soit supprimée
			} else {
				synchronized (this.zk_client) {
					this.zk_client.wait();
				}
			}
		}
		// NB : j'ai remarqué que le test est validé selon le temps d'exécution du test.
		// peut etre que le Thread.sleep(500) n'est pas assez long.
	}
}
