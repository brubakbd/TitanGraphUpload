package edu.rosehulman.brubakbd;
import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

public class TitanMRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] vals = line.split("\t");
		
		BaseConfiguration conf = new BaseConfiguration();
				conf.setProperty("gremlin.graph", "com.thinkaurelius.titan.core.TitanFactory");
				conf.setProperty("storage.backend", "hbase");
				conf.setProperty("storage.hostname", "hadoop-16.csse.rose-hulman.edu");
				conf.setProperty("storage.batch-loading", true);
				conf.setProperty("storage.hbase.ext.zookeeper.znode.parent","/hbase-unsecure");
				conf.setProperty("storage.hbase.ext.hbase.zookeeper.property.clientPort", 2181);
				conf.setProperty("cache.db-cache",true);
				conf.setProperty("cache.db-cache-clean-wait", 20);
				conf.setProperty("cache.db-cache-time", 180000);
				conf.setProperty("cache.db-cache-size", 0.5);
		
				
		TitanGraph graph = TitanFactory.open(conf);
		TitanVertex v1 = graph.addVertex();
		v1.property("pageID", vals[0]);
		TitanVertex v2 = graph.addVertex();
		v2.property("pageID", vals[1]);
		
		v1.addEdge("links_To", v2);
		
		graph.tx().commit();
	}
}
