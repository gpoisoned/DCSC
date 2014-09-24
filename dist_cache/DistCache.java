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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class DistCache extends Configured implements Tool{

  public static class DistMapper extends Mapper<Text, Text, Text, IntWritable>{
    private Text word = new Text();
    private BufferedReader  brReader;
    private String LookupValue = "";
    private static HashMap<String,String> Cache = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        System.out.println("In Setup: " + cacheFilesLocal.toString());
        if (cacheFilesLocal != null){
            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("cache_file")) {
                    loadCache(context);
                }
            }
        }
        else{
            System.out.println("Null error in cacheFilesLocal\n");
        }
  
    }

    private void loadCache(Context context) throws IOException{
        String strLineRead = "";
        System.out.println("Printing from loadCache: ");
        try {
            brReader = new BufferedReader(new FileReader("cache_file"));
            // Read each line, split and load to HashMap
            while ((strLineRead = brReader.readLine()) != null) {
                String temp[] = strLineRead.split(",");
                Cache.put(temp[0].trim(),temp[1].trim());
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
        word.set(key);
        try{
            LookupValue = Cache.get(key.toString());
        } finally{
            if (LookupValue == null){ 
                LookupValue = "0";
            }
        }

        IntWritable count = new IntWritable(Integer.parseInt(value.toString()) + 
                            Integer.parseInt(LookupValue));
        context.write(key, count);
        LookupValue = "";
    }
  }

  public static class DistReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable value: values){
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
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
         "0:1");

    DistributedCache.addCacheFile(new URI("/user/user/cache/cache_file#cache_file"),conf);
    job.setJarByClass(DistCache.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapperClass(DistMapper.class);
    job.setReducerClass(DistReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(),
        new DistCache(), args);
    System.exit(exitCode);
  }
}
