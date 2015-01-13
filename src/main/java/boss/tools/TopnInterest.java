package boss.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by henriezhang on 2015/1/9.
 */

public class TopnInterest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.map.failures.percent", "3");
        conf.set("mapred.job.queue.name", "gboss");
        conf.set("mapred.queue.name", "gboss");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: hadoop jar usermodel.jar boss.tools.TopnInterest <in> <out> <topn>");
            System.exit(3);
        }
        String inPath = otherArgs[0];
        String outPath = otherArgs[1];
        String topN = otherArgs[2];

        conf.set("boss.tools.topn", topN);
        Job job = new Job(conf, "TopnInterest");
        job.setJarByClass(TopnInterest.class);
        job.setMapperClass(TopnInterestMapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);
        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定输入路径
        Path iPath = new Path(inPath);
        FileInputFormat.addInputPath(job, iPath);

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        // 如果输出路径已经存在则清除之
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TopnInterestMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        // 计算数据的斜率
        private int topN = 3;

        // 初始化数据
        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            // 获取斜率
            /*try {
                String tmp = conf.get("boss.tools.topn");
                if (tmp != null) {
                    topN = Integer.parseInt(tmp);
                }
            } catch (Exception e) {
            }*/
        }

        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            // 判断字段数个数是否合法
            if (fields.length < 2 ) {
                return;
            }

            Map<String, Double> map = new HashMap<String, Double>();
            String items[] = fields[1].split(",");
            for(int i=0; i<items.length; i++) {
                String it[] = items[i].split(":");
                if(it.length>=2) {
                    try{
                        Double value = Double.parseDouble(it[1]);
                        map.put(it[0], value);
                    } catch(Exception e) {

                    }
                }
            }

            List<Map.Entry<String, Double>> infoIds =
                    new ArrayList<Map.Entry<String, Double>>(map.entrySet());

            Collections.sort(infoIds, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    int mark = 0;
                    if (o2.getValue() - o1.getValue() > 0) {
                        mark = 1;
                    } else if (o2.getValue() - o1.getValue() < 0) {
                        mark = -1;
                    }
                    return mark;
                }
            });

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < infoIds.size() && i<3; i++) {
                Map.Entry<String, Double> it =  infoIds.get(i);
                sb.append(",");
                sb.append(it.getKey());
                sb.append(":");
                sb.append(it.getValue());
            }
            context.write(new Text(fields[0]), new Text(sb.substring(1)));

            /*String interest = fields[1];
            int idx = topN;
            int pos = -1, lastPos = -1;
            while(idx-- > 0) {
                pos = interest.indexOf(",", lastPos+1);
                if(pos==-1) {
                    break;
                }
                lastPos = pos;
            }

            if(pos>0) {
                interest = interest.substring(0, pos-1);
            }
            context.write(new Text(fields[0]), new Text(interest));*/
        }
    }
}

