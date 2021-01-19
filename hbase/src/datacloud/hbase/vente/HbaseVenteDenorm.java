package datacloud.hbase.vente;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseVenteDenorm {
	
	private static final TableName TABLE_CLIENT = TableName.valueOf("client");
	private static final TableName TABLE_MAG = TableName.valueOf("magasin");
	private static final TableName TABLE_CAT = TableName.valueOf("categorie");
	private static final TableName TABLE_PROD = TableName.valueOf("produit");
	private static final TableName TABLE_PROD2 = TableName.valueOf("produit2");
	private static final TableName TABLE_VENTE = TableName.valueOf("vente");
	private static final TableName TABLE_VENTE2 = TableName.valueOf("vente2");
	
    private static final byte[] CF_NAME = Bytes.toBytes("defaultcf");
	
	//1er denormalisation : fusion des tables Produit et Catégorie
    public static void denorm_fusiontable(Connection c, Admin admin) throws IOException {

        if(!admin.tableExists(TABLE_PROD2)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_PROD2)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_NAME))
                    .build();
            admin.createTable(desc);
            System.out.println("La table 'produit2' a été créée avec succès !");
            
        	Table t_produit = c.getTable(TABLE_PROD);
        	Table t_categorie = c.getTable(TABLE_CAT);
        	Table t_produit2 = c.getTable(TABLE_PROD2);
            
        	//Parcours de la table Produit
        	Scan scan = new Scan();
        	scan.addColumn(CF_NAME, Bytes.toBytes("idprod"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("designation"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("prix"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("categorie"));
        	
        	ResultScanner results = t_produit.getScanner(scan);
        	
        	for(Result r : results) {
        		
        		String id_prod = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("idprod")));
        		String designation = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("designation")));
        		Integer prix = Bytes.toInt(r.getValue(CF_NAME, Bytes.toBytes("prix")));
        		String id_cat = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("categorie")));
        		
        		// Obtenir le nom de la categorie dans la table Categorie à partir de l'id_cat
        		Get get_nomcat = new Get(Bytes.toBytes(id_cat));
        		get_nomcat.addColumn(CF_NAME,Bytes.toBytes("designation"));
        		Result res_nomcat = t_categorie.get(get_nomcat);
        		String nom_cat = Bytes.toString(res_nomcat.getValue(CF_NAME, Bytes.toBytes("designation")));
        		
        		// creation colonnes + insertion valeurs dans la table produit2
        		Put put = new Put(Bytes.toBytes(id_prod));
				put.addColumn(CF_NAME, Bytes.toBytes("idprod"), Bytes.toBytes(id_prod));
				put.addColumn(CF_NAME, Bytes.toBytes("designation"), Bytes.toBytes(designation));
				put.addColumn(CF_NAME, Bytes.toBytes("prix"), Bytes.toBytes(prix));
				put.addColumn(CF_NAME, Bytes.toBytes("categorie"), Bytes.toBytes(id_cat));
				//ajout de la nouvelle colonne de la table Categorie
				put.addColumn(CF_NAME, Bytes.toBytes("designation_cat"), Bytes.toBytes(nom_cat));
        		
				t_produit2.put(put);
        	}
        	System.out.println("La table 'produit2' a été remplie avec succès !");
        } else {
        	System.out.println("La table 'produit2' existe déjà !");
        }
    }
    
    // 2e denormalisation : ajout de la clé primaire de Categorie dans Vente
    public static void denorm_add_column(Connection c, Admin admin) throws IOException {
        if(!admin.tableExists(TABLE_VENTE2)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_VENTE2)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_NAME))
                    .build();
            admin.createTable(desc);
            System.out.println("La table 'vente2' a été créée avec succès !");
            
        	Table t_produit2 = c.getTable(TABLE_PROD2);
        	Table t_vente = c.getTable(TABLE_VENTE);
        	Table t_vente2 = c.getTable(TABLE_VENTE2);
            
        	Scan scan = new Scan();
        	scan.addColumn(CF_NAME, Bytes.toBytes("idvente"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("client"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("produit"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("magasin"));
        	scan.addColumn(CF_NAME, Bytes.toBytes("date"));
        	
        	ResultScanner results = t_vente.getScanner(scan);
        	
        	for(Result r : results) {
        		
        		String id_vente = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("idvente")));
        		String client = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("client")));
        		String produit = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("produit")));
        		String magasin = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("magasin")));
        		String date = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("date")));
        		
        		
        		Get get_idcat = new Get(Bytes.toBytes(produit));
        		get_idcat.addColumn(CF_NAME,Bytes.toBytes("categorie"));
        		Result res_idcat = t_produit2.get(get_idcat);
        		String id_cat = Bytes.toString(res_idcat.getValue(CF_NAME, Bytes.toBytes("categorie")));
        		
        		// creation de la nouvelle table produit2
        		Put put = new Put(Bytes.toBytes(id_vente));
				put.addColumn(CF_NAME, Bytes.toBytes("idvente"), Bytes.toBytes(id_vente));
				put.addColumn(CF_NAME, Bytes.toBytes("client"), Bytes.toBytes(client));
				put.addColumn(CF_NAME, Bytes.toBytes("produit"), Bytes.toBytes(produit));
				put.addColumn(CF_NAME, Bytes.toBytes("magasin"), Bytes.toBytes(magasin));
				put.addColumn(CF_NAME, Bytes.toBytes("date"), Bytes.toBytes(date));
				put.addColumn(CF_NAME, Bytes.toBytes("idcategorie"), Bytes.toBytes(id_cat));
        		
				t_vente2.put(put);
        	}
        	System.out.println("La table 'vente2' a été remplie avec succès !");
        } else {
        	System.out.println("La table 'vente2' existe déjà !");
        }
    }
    
    
    public static HashMap<String, Integer> getNbVentePerCat1(Connection c) throws IOException {
    	HashMap<String, Integer> result = new HashMap<String, Integer>();
    	
    	Table t_vente = c.getTable(TABLE_VENTE);
    	Table t_produit2 = c.getTable(TABLE_PROD2);
    	
    	Scan scan = new Scan();
    	scan.addColumn(CF_NAME, Bytes.toBytes("produit"));
    	
    	ResultScanner results = t_vente.getScanner(scan);
    	
    	for(Result r : results) {
    		String id_prod = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("produit")));
    		
    		Get get_nomcat = new Get(Bytes.toBytes(id_prod));
    		get_nomcat.addColumn(CF_NAME,Bytes.toBytes("designation_cat"));
    		Result res_nomcat = t_produit2.get(get_nomcat);
    		String nom_cat = Bytes.toString(res_nomcat.getValue(CF_NAME, Bytes.toBytes("designation_cat")));
    		
    		
    		if(result.containsKey(nom_cat)) {
    			result.put(nom_cat,result.get(nom_cat)+1);
    		} else {
    			result.put(nom_cat, 1);
    		}
    	}
    	
    	return result;
    }
    
    
    public static HashMap<String, Integer> getNbVentePerCat2(Connection c) throws IOException {
    	HashMap<String, Integer> result = new HashMap<String, Integer>();
    	
    	Table t_vente2 = c.getTable(TABLE_VENTE2);
    	Table t_categorie = c.getTable(TABLE_CAT);
    	
    	Scan scan = new Scan();
    	scan.addColumn(CF_NAME, Bytes.toBytes("idcategorie"));
    	
    	ResultScanner results = t_vente2.getScanner(scan);
    	
    	for(Result r : results) {
    		String id_cat = Bytes.toString(r.getValue(CF_NAME, Bytes.toBytes("idcategorie")));
    		
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
		Admin admin = c.getAdmin();
		
		denorm_fusiontable(c, admin);
		denorm_add_column(c, admin);
		
		long startTime1 = System.currentTimeMillis();
		HashMap<String, Integer> results1 = getNbVentePerCat1(c);
		long endTime1 = System.currentTimeMillis();
		System.out.println("Temps d'exécution (version denorm1) = " + (endTime1 - startTime1)/1000.0 + " seconds");
		showResult(results1);
		
		long startTime2 = System.currentTimeMillis();
		HashMap<String, Integer> results2 = getNbVentePerCat2(c);
		long endTime2 = System.currentTimeMillis();
		System.out.println("Temps d'exécution (version denorm2) = " + (endTime2 - startTime2)/1000.0 + " seconds");
		showResult(results2);
		
    }
 
}
