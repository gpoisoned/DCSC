import java.io.*;
import java.util.HashMap;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class Patent extends Configured implements Tool{

  public static class FirstMapper extends Mapper<Text, Text, Text, Text>{
    private BufferedReader  brReader;
    private Text output;
    private String citedState = "";
    private String citingState = "";
    private String outputValue = "";
    private static HashMap<String,String> Cache = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
     
        if (cacheFilesLocal != null){
            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("apat63_99.txt")) {
                    loadCache(context);
                }
            }
        }
  
    }

    private void loadCache(Context context) throws IOException{
        String strLineRead = "";
        try {
            brReader = new BufferedReader(new FileReader("apat63_99.txt"));
            // Read each line, split and load to HashMap
            int linecount = 0;
            while ((strLineRead = brReader.readLine()) != null) {
                // Ignore first line which defines the schema of the CSV file
                if (linecount > 0){
                    String temp[] = strLineRead.split(",");
                    // Index 0 => Patent Number, Index 5 => State
                    Cache.put(temp[0].trim(),temp[5].trim()); 
                }
                linecount++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException{
        // Prepare intermediate result in the format of: Cited, State, Citing, State
        // using distributed cache for the lookup.
        String citedPatent = key.toString();
        String citingPatent = value.toString();

        if (citedPatent != "CITED"){
            try{
                citedState = Cache.get(citedPatent);
                citingState = Cache.get(citingPatent);
            } finally{
                // Make sure that we don't get null pointer exception
                if ((citingState == null) || citingState.isEmpty()){ 
                    citingState = "";
                }
                else if ((citedState == null) || citedState.isEmpty()){
                    citedState = "";
                }
            }
            // Build the value string
            outputValue = citedState + "," + citingPatent +"," + citingState;
            output = new Text (outputValue);
            String temp = citedPatent + ",";
            context.write(new Text(temp), output);
        }   
    }
  }

  @Override
  public int run(String[] args ) throws Exception {
    Job job = new Job(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName("dist cache");

    conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
    conf.set("mapreduce.fieldsel.data.field.separator",",");
    conf.set("mapreduce.fieldsel.map.output.key.value.fields.spec",
         "1:0");

    DistributedCache.addCacheFile(new URI("/user/user/cache/apat63_99.txt"),conf);
    job.setJarByClass(Patent.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapperClass(FirstMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(),
        new Patent(), args);
    System.exit(exitCode);
  }
}
