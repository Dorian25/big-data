package datacloud.hbase.vente;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseVente {
	private static final TableName TABLE_CLIENT = TableName.valueOf("client");
	private static final TableName TABLE_MAG = TableName.valueOf("magasin");
	private static final TableName TABLE_CAT = TableName.valueOf("categorie");
	private static final TableName TABLE_PROD = TableName.valueOf("produit");
	private static final TableName TABLE_VENTE = TableName.valueOf("vente");
	
    private static final byte[] CF_NAME = Bytes.toBytes("defaultcf");
    
    
    
    public static HashMap<String, Integer> getNbVentePerCat(Connection c) throws IOException {
    	HashMap<String, Integer> result = new HashMap<String, Integer>();
    	
    	Table t_vente = c.getTable(TABLE_VENTE);
    	Table t_produit = c.getTable(TABLE_PROD);
    	Table t_categorie = c.getTable(TABLE_CAT);
    	
    	Scan scan = new Scan();
    	scan.addColumn(CF_NAME, Bytes.toBytes("produit"));
    	
    	ResultScanner results = t_vente.getScanner(scan);
    	
    	for(Result r : results) {
    		String id_prod = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("produit")));
    		
    		Get get_idcat = new Get(Bytes.toBytes(id_prod));
    		get_idcat.addColumn(CF_NAME,Bytes.toBytes("categorie"));
    		Result res_idcat = t_produit.get(get_idcat);
    		String id_cat = Bytes.toString(res_idcat.getValue(CF_NAME,Bytes.toBytes("categorie")));
    		
    		Get get_nomcat = new Get(Bytes.toBytes(id_cat));
    		get_nomcat.addColumn(CF_NAME,Bytes.toBytes("designation"));
    		Result res_nomcat = t_categorie.get(get_nomcat);
    		String nom_cat = Bytes.toString(res_nomcat.getValue(CF_NAME, Bytes.toBytes("designation")));
    		
    		
    		if(result.containsKey(nom_cat)) {
    			result.put(nom_cat,result.get(nom_cat)+1);
    		} else {
    			result.put(nom_cat, 1);
    		}
    	}
    	
    	return result;
    }
    
    public static void showResult(HashMap<String, Integer> hashmap) {
    	for (String name: hashmap.keySet()){
            String key = name.toString();
            String value = hashmap.get(name).toString();  
            System.out.println(key + " " + value);  
    	} 
    }
    
    public static void main(String[] args) throws IOException {
    	Configuration config = HBaseConfiguration.create();
		Connection c = ConnectionFactory.createConnection(config);
		
		long startTime = System.currentTimeMillis();
		HashMap<String, Integer> results = getNbVentePerCat(c);
		long endTime = System.currentTimeMillis();
		System.out.println("Temps d'exécution (version normalisé)= " + (endTime - startTime)/1000.0 + " seconds");
		showResult(results);
    }
}
