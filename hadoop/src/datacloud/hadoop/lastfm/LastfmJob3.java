package datacloud.hadoop.lastfm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LastfmJob3 {
	
	public static class Job1Mapper extends Mapper<LongWritable, Text, Text, TripleIntWritable>{
		
		//output
		private Text trackId = new Text();
		private TripleIntWritable outputj1 = new TripleIntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// exemple d'une ligne: track0	3
			String[] lines = value.toString().split(System.getProperty("line.separator"));
			
			for (String l : lines) {
				String[] tokens = l.split("\t");
				
				String t_id = tokens[0];
				Integer nb_listener = Integer.parseInt(tokens[1]);
				
				trackId.set(t_id);;
				outputj1.setInt1(nb_listener);
				context.write(trackId, outputj1);
			}
		}
	}
	

	
	public static class Job2Mapper extends Mapper<LongWritable, Text, Text, TripleIntWritable>{
		
		//output
		private Text trackId = new Text();
		private TripleIntWritable outputj2 = new TripleIntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// exemple d'une ligne : track0	  39 10
			String[] lines = value.toString().split(System.getProperty("line.separator"));
			
			for (String l : lines) {
				String[] tokens = l.split("\t");
				String[] val = tokens[1].split(" ");
				
				String t_id = tokens[0];
				Integer nb_listening = Integer.parseInt(val[0]);
				Integer nb_skips = Integer.parseInt(val[1]);
				
				trackId.set(t_id);
				outputj2.setInt2(nb_listening);
				outputj2.setInt3(nb_skips);
				context.write(trackId, outputj2);
			}
		}
	}
	
	public static class LastfmJob3Reducer extends Reducer<Text,TripleIntWritable,Text,TripleIntWritable> {
		
		private TripleIntWritable outputj3 = new TripleIntWritable();
		public void reduce(Text key, Iterable<TripleIntWritable> values, Context context) throws IOException, InterruptedException {
			int cpt_listener = 0;
			int cpt_listening = 0;
			int cpt_skip = 0;
			
			for (TripleIntWritable v : values) {
				cpt_listener += v.getInt1();
				cpt_listening += v.getInt2();
				cpt_skip += v.getInt3();
			}
			outputj3.setInt1(cpt_listener);
			outputj3.setInt2(cpt_listening);
			outputj3.setInt3(cpt_skip);
			context.write(key, outputj3);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: lastfmjob3 <in1> <in2> <out>");
		    System.exit(2);
		}
		//job 1
		Job job = Job.getInstance(conf, "lastfmjob3");
		job.setJarByClass(LastfmJob3.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		job.setReducerClass(LastfmJob3Reducer.class); // indique la classe du Reducer   
		job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
		job.setOutputValueClass(TripleIntWritable.class);// indique la classe  de la clé de sortie reduce
		job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Job1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Job2Mapper.class);
	
		final Path outDir = new Path(otherArgs[2]);//indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}
		   
		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}
}
