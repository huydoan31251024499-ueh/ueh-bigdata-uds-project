import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ASEANCaseCount {

    public static class CaseMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text region = new Text("South-East Asia Region");
        private DoubleWritable cases = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");

            if (columns.length > 2 && !columns[0].equals("Name") && !columns[0].equals("Global")) {
                String whoRegion = columns[1].trim();
                
                if (whoRegion.equalsIgnoreCase("South-East Asia")) {
                    try {
                        double cumulativeCases = Double.parseDouble(columns[2].replace(",", ""));
                        cases.set(cumulativeCases);
                        context.write(region, cases);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
    }

    public static class CaseReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ASEAN Cumulative Case Count");
        job.setJarByClass(ASEANCaseCount.class);
        job.setMapperClass(CaseMapper.class);
        job.setCombinerClass(CaseReducer.class);
        job.setReducerClass(CaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        if (args.length >= 2) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } else {
            FileInputFormat.addInputPath(job, new Path("lab03/input/"));
            FileOutputFormat.setOutputPath(job, new Path("lab03/output-java/"));
        }
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}