import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HadoopPageRankResultMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if ( value == null || value.getLength() == 0 ) {
    		return;
    	}
    	
        int tabIdx1 = value.find("\t");
        int tabIdx2 = value.find("\t", tabIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tabIdx1);
        String pageRank = Text.decode(value.getBytes(), tabIdx1 + 1, tabIdx2 - (tabIdx1 + 1));
        String outlinks = Text.decode(value.getBytes(), tabIdx2 + 1, value.getLength() - (tabIdx2 + 1));

		 String[] allNextPages = outlinks.split(",");
	        if ( allNextPages == null || allNextPages.length == 0 ) {
	        	return;
	        }

		double currentPR = Double.parseDouble(pageRank.toString());
        int totalNumOfNextPages = allNextPages.length;
        for (String nextPage : allNextPages) { 
        	Text rankContribution = new Text(currentPR/totalNumOfNextPages + "");
            context.write(new Text(nextPage), rankContribution); 
        }
}
}
	

