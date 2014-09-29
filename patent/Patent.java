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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;

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
            outputValue = citedPatent + ","+ citedState + "," + citingPatent +"," + citingState;
            output = new Text (outputValue);
            String temp = citedPatent + ",";
            context.write(new Text(temp), output);
        }   
    }
  }

  // The schema of the input file for SecondMapper is:
  // Cited, Cited State, Citing, Citing State
  // The output of SecondMapper is:
  // Citing State, Cited State
  public static class SecondMapper extends Mapper<Object, Text, Text, Text>{
    private Text outKey;
    private Text outValue;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        if (line.length == 5){
            outKey = new Text(line[4]);
            outValue = new Text(line[2]);
            context.write(outKey, outValue);
        }
    }
  }

  public static class SecondReducer extends Reducer<Text, Text, Text, FloatWritable>{
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int total = 0;
        int sameState = 0;
        String keyStr = key.toString();
        String vals  = "";
        for (Text val : values) {
            if (keyStr.equals(val.toString())){
                // Found same state citation
                sameState +=1;
            }
            total +=1;
        }
        float percent = (float) sameState / total;
        result = new FloatWritable(percent);
        context.write(key, result);
    }
  }

  @Override
  public int run(String[] args ) throws Exception {
    
    Job chainJob = new Job(getConf());
    Configuration chainConf = chainJob.getConfiguration();
    chainJob.setJobName("dist-cache-multi-mapreduce");

    String interDir = "intermediate-output";
    FileSystem fs = FileSystem.get(chainConf);
    Path interPath = new Path( interDir );
    fs.delete(interPath, true); // recursive delete 
    // fs.mkdirs(interPath); -- do no create, automatically created...

    chainConf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
    chainConf.set("mapreduce.fieldsel.data.field.separator",",");
    chainConf.set("mapreduce.fieldsel.map.output.key.value.fields.spec",
         "1:0");
    DistributedCache.addCacheFile(new URI("/user/user/cache/apat63_99.txt"),chainConf);

    chainJob.setJarByClass(Patent.class); 
    
    chainJob.setInputFormatClass(KeyValueTextInputFormat.class);
    chainJob.setMapperClass(FirstMapper.class);
    chainJob.setMapOutputKeyClass(Text.class);
    chainJob.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(chainJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(chainJob, interPath);

    boolean success;
    success = chainJob.waitForCompletion(true);
    if (success == false){
        return 1;
    }

    Configuration secondChainConf = new Configuration();
    Job secondChainJob = Job.getInstance(secondChainConf,
                         "Second-map-reduce-job");
    secondChainJob.setJarByClass(Patent.class);

    secondChainJob.setMapperClass(SecondMapper.class);
    secondChainJob.setReducerClass(SecondReducer.class);

    secondChainJob.setOutputKeyClass(Text.class);
    secondChainJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(secondChainJob, interPath);
    FileOutputFormat.setOutputPath(secondChainJob, new Path(args[1]));

    success = secondChainJob.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(),
        new Patent(), args);
    System.exit(exitCode);
  }
}
