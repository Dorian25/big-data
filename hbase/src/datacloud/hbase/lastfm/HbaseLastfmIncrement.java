package datacloud.hbase.lastfm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseLastfmIncrement {
    private static final TableName TABLE_NAME = TableName.valueOf("ecoute");
    private static final byte[] CF_NAME = Bytes.toBytes("cf1");
	
    public static Boolean createTable(Admin admin) throws IOException {
        if(!admin.tableExists(TABLE_NAME)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_NAME))
                    .build();
            admin.createTable(desc);
            System.out.println("La table 'ecoute' a été créée avec succès !");
            return true;
        } else {
        	System.out.println("La table 'ecoute' existe déjà !");
        	return false;
        }
    }
	
	
	public static void main(String[] args) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		Connection c = ConnectionFactory.createConnection(config);
		Admin admin = c.getAdmin();
		
		System.out.println("Connection réussie");
		
		// il faut à chaque fois exécuter ces cmd dans le 'hbase shell' 
		// puis disable 'ecoute' => drop 'ecoute'
		
		Boolean response = createTable(admin);
		
		if(response) {
			Table t_ecoute = c.getTable(TABLE_NAME);
			
			System.out.println("Lecture du fichier de données...");
			
			BufferedReader br = new BufferedReader(new FileReader("resources/lastfm_fichier_1"));
			String line;
			int i = 0;
			
			while((line = br.readLine()) != null) {
				// decomposition de la ligne en colonne	
				String[] cols = line.split(" ");
				System.out.println("Line"+i+": "+cols[0]+" "+cols[1]+" "+cols[2]+" "+cols[3]+" "+cols[4]);
				
				Put put = new Put(Bytes.toBytes(cols[0]+cols[1]));	
				Get get = new Get(Bytes.toBytes(cols[0]+cols[1]));
				
				Result res = t_ecoute.get(get);
					
				if(!t_ecoute.get(get).isEmpty()) {
					System.out.println("Update/Increment line");
					
					Increment inc_local = new Increment(Bytes.toBytes(cols[0]+cols[1]));
					Increment inc_radio = new Increment(Bytes.toBytes(cols[0]+cols[1]));
					Increment inc_skip = new Increment(Bytes.toBytes(cols[0]+cols[1]));
					
					byte[] localListening = res.getValue(CF_NAME, Bytes.toBytes("localListening"));
					byte[] radioListening = res.getValue(CF_NAME, Bytes.toBytes("radioListening"));
					byte[] skip = res.getValue(CF_NAME, Bytes.toBytes("skip"));
					
					inc_local.addColumn(CF_NAME, Bytes.toBytes("localListening"), Bytes.toLong(localListening));
					inc_radio.addColumn(CF_NAME, Bytes.toBytes("radioListening"), Bytes.toLong(radioListening));
					inc_skip.addColumn(CF_NAME, Bytes.toBytes("skip"), Bytes.toLong(skip));
					
					t_ecoute.increment(inc_local);
					t_ecoute.increment(inc_radio);
					t_ecoute.increment(inc_skip);

				} else {
					put.addColumn(CF_NAME, Bytes.toBytes("userID"), Bytes.toBytes(cols[0]));
					put.addColumn(CF_NAME, Bytes.toBytes("trackID"), Bytes.toBytes(cols[1]));
					put.addColumn(CF_NAME, Bytes.toBytes("localListening"), Bytes.toBytes(Long.parseLong(cols[2])));
					put.addColumn(CF_NAME, Bytes.toBytes("radioListening"), Bytes.toBytes(Long.parseLong(cols[3])));
					put.addColumn(CF_NAME, Bytes.toBytes("skip"), Bytes.toBytes(Long.parseLong(cols[4])));
					
					t_ecoute.put(put);
				}
				// ajout de la ligne
				i += 1;    
			}
			
			System.out.println("Nombre de lignes lues : "+i);
			br.close();
		}
		
		admin.close();
		c.close();
	}
}
