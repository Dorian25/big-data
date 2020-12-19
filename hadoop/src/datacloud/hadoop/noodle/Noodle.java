package datacloud.hadoop.noodle;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.Partitioner;

public class Noodle {
	
	private static int NB_REDUCE = 12;
	
	public static class NoodleMapper extends Mapper<LongWritable, Text, PairIntWritable, Text>{
		//05_08_2018_02_21 208.204.58.126 motcles.list38+motcles.list4 
		
		//output
		private PairIntWritable month_tranche = new PairIntWritable();
		private Text keywords = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lines = value.toString().split(System.getProperty("line.separator"));
			
			for (String l : lines) {
				String[] tokens = l.split(" ");
				
				String[] date = tokens[0].split("_");
				String kw = tokens[2];
				
				Integer mois = Integer.parseInt(date[1]);
				Integer heure = Integer.parseInt(date[3]);
				Integer minute = Integer.parseInt(date[4]);
		
				// division entiere avec les minutes
				month_tranche.setInt1(2*heure + minute/30);
				month_tranche.setInt2(mois);
				keywords.set(kw);
				context.write(month_tranche, keywords);
			}
		}
	}
	
	public static class NoodleReducer extends Reducer<PairIntWritable,Text,IntWritable,PairIntWritableText> {
		
		private IntWritable tranche = new IntWritable();
		private PairIntWritableText result = new PairIntWritableText();

		public void reduce(PairIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> dic = new HashMap<String, Integer>();
			int nb_query = 0;
			String temp = "";
	
			for (Text q : values) {
				nb_query++;
	
				StringTokenizer itr = new StringTokenizer(q.toString(),"+");
			    while (itr.hasMoreTokens()) {
			    	String current_w = itr.nextToken();
			    	

			    	if(dic.containsKey(current_w)) {
			    		dic.put(current_w, dic.get(current_w) + 1);
			    	} else  {
			    		dic.put(current_w, 1);
			    	}
			    	
			    	if(temp.isEmpty()){
			    		temp = current_w;
			    	} else {
			    		if(dic.get(current_w) > dic.get(temp)) {
			    			temp = current_w;
			    		}
			    	}
			    }
			}
			tranche.set(key.getInt1());
			result.setValue1(nb_query);
			result.setValue2(temp);
			context.write(tranche, result);
		}
		
	}
	
	public static class NoodlePartitioner extends Partitioner < PairIntWritable, Text > {
		@Override
		public int getPartition(PairIntWritable arg0, Text arg1, int nb_reduce) {
			return arg0.getInt2()-1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: noodle <in> <out>");
		    System.exit(2);
		}
		Job job = Job.getInstance(conf, "noodle");
		job.setJarByClass(Noodle.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		job.setMapperClass(NoodleMapper.class); // indique la classe du Mapper
		job.setReducerClass(NoodleReducer.class); // indique la classe du Reducer
		job.setMapOutputKeyClass(PairIntWritable.class);// indique la classe  de la clé sortie map
		job.setMapOutputValueClass(Text.class);// indique la classe  de la valeur sortie map    
		job.setOutputKeyClass(IntWritable.class);// indique la classe  de la clé de sortie reduce    
		job.setOutputValueClass(PairIntWritableText.class);// indique la classe  de la clé de sortie reduce
		job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		job.setPartitionerClass(NoodlePartitioner.class);// indique la classe du partitionneur
		job.setNumReduceTasks(NB_REDUCE);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		    
		    
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
		final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}
		   
		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}

}
