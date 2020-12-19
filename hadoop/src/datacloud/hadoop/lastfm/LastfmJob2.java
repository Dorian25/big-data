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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import datacloud.hadoop.noodle.PairIntWritable;

public class LastfmJob2 {
	public static class LastfmJob2Mapper extends Mapper<LongWritable, Text, Text, PairIntWritable>{
		//user2 track2 1 6 2 
		//user0 track4 8 9 2 
		
		//output
		private Text trackId = new Text();
		private PairIntWritable nblisten_skip = new PairIntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lines = value.toString().split(System.getProperty("line.separator"));
			
			for (String l : lines) {
				String[] tokens = l.split(" ");
				
				String t_id = tokens[1];
				
				Integer local = Integer.parseInt(tokens[2]);
				Integer radio = Integer.parseInt(tokens[3]);
				Integer skips = Integer.parseInt(tokens[4]);
				
				trackId.set(t_id);
				nblisten_skip.setInt1(local+radio);
				nblisten_skip.setInt2(skips);
				
				context.write(trackId, nblisten_skip);
			}
		}
	}
	
	public static class LastfmJob2Reducer extends Reducer<Text,PairIntWritable,Text,PairIntWritable> {
		
		private PairIntWritable nblisten_skip = new PairIntWritable();
		public void reduce(Text key, Iterable<PairIntWritable> values, Context context) throws IOException, InterruptedException {
			int cpt_listening = 0;
			int cpt_skip = 0;
			for (PairIntWritable v : values) {
				cpt_listening += v.getInt1();
				cpt_skip += v.getInt2();
			}
			nblisten_skip.setInt1(cpt_listening);
			nblisten_skip.setInt2(cpt_skip);
			context.write(key, nblisten_skip);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: lastfmjob2 <in> <out>");
		    System.exit(2);
		}

		Job job = Job.getInstance(conf, "lastfmjob2");
		job.setJarByClass(LastfmJob2.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		job.setMapperClass(LastfmJob2Mapper.class); // indique la classe du Mapper
		job.setReducerClass(LastfmJob2Reducer.class); // indique la classe du Reducer
		job.setMapOutputKeyClass(Text.class);// indique la classe  de la clé sortie map
		job.setMapOutputValueClass(PairIntWritable.class);// indique la classe  de la valeur sortie map    
		job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
		job.setOutputValueClass(PairIntWritable.class);// indique la classe  de la clé de sortie reduce
		job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
		final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}
		   
		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}
	
	// ne pas oublier de créer un dossier /output_j2 et supprimer le fichier SUCCESS
}
