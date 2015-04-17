package boss.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserColumnPv {
    public static class SplitMapper
            extends Mapper<Object, Text, Text, Text> {
        Pattern p;
        Matcher m;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            p = Pattern.compile("^\\d{5,12}$");
        }

        private boolean isValidQQ(String qq) {
            // 判断QQ是否合法
            m = p.matcher(qq);
            return (m.find()) ? true : false;
        }

        @Override
        public void map(Object key, Text inValue, Context context) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\\||,");
            if (fields.length >= 17) { // PGV 数据
                if (!isValidQQ(fields[16])) {
                    return;
                }

                String siteValue = null;
                int pos1 = fields[9].indexOf("L.");
                if (pos1 >= 0) { // 普通PGV数据
                    int pos2 = fields[9].indexOf("_", pos1);
                    if (pos2 > pos1) {
                        String column = fields[9].substring(pos1 + 2, pos2);
                        if (column.startsWith("news.milite")) {
                            siteValue = "milite";
                        } else if (column.startsWith("news.newssh")) {
                            siteValue = "society";
                        } else {
                            String[] cls = column.split("\\.");
                            if (cls.length > 0) {//just keep site, don't take into account all columns
                                siteValue = cls[0].trim();
                            }
                        }
                    }
                } else {  // 特殊PGV数据
                    String curDomain = fields[3];
                    if (curDomain.equals("film.qq.com") || curDomain.equals("v.qq.com")) {
                        siteValue = "v";
                    } else if (curDomain.equals("dajia.qq.com")) {
                        siteValue = "dajia";
                    }
                }

                if (siteValue != null) {
                    String strValue = siteValue + "|" + fields[3] + fields[4];
                    String normValue = strValue.replaceAll("-", "_"); //中划线改成下划线
                    context.write(new Text(fields[16]), new Text(normValue));
                }
            } else if(fields.length >= 13) { // AIO 点击数据
                if (!isValidQQ(fields[2])) {
                    return;
                }

                String url = null;
                if(fields[9].startsWith("%u7F51%u8D2D")) {  // 老版网购数据
                    url = fields[10];
                } else if (fields[9].equals("buy")) { // 新版网购数据
                    url = fields[8];
                }

                if(url!=null) { // 如果是合法的网购数据
                    String normValue = "wanggou|x" + url.hashCode(); // 将url地址转化为x+hashcode
                    context.write(new Text(fields[2]), new Text(normValue));
                }
            }
        }
    }

    public static class SplitCombiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text Id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map mapUrl = new HashMap();
            for (Text val : values) {
                Object num = mapUrl.get(val.toString());
                if (num == null) {
                    mapUrl.put(val.toString(), 1);
                }
            }
            Iterator it = mapUrl.entrySet().iterator();
            while (it.hasNext()) {
                Entry entryColumn = (Entry) it.next();
                context.write(Id, new Text((String) entryColumn.getKey()));
            }
        }
    }

    public static class SplitReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text Id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map mapUrl = new HashMap();
            for (Text val : values) {
                Object num = mapUrl.get(val.toString());
                if (num == null) {
                    mapUrl.put(val.toString(), 1);
                }
            }
            Map mapColumn = new HashMap();
            Iterator it = mapUrl.entrySet().iterator();
            while (it.hasNext()) {
                Entry entryColumn = (Entry) it.next();
                String[] cls = ((String) entryColumn.getKey()).split("\\|");
                Object num = mapColumn.get(cls[0]);
                if (num == null) {
                    mapColumn.put(cls[0], 1);
                } else {
                    mapColumn.put(cls[0], (Integer) num + 1);
                }
            }

            Iterator itCl = mapColumn.entrySet().iterator();
            while (itCl.hasNext()) {
                Entry entryColumn = (Entry) itCl.next();
                context.write(Id, new Text((String) entryColumn.getKey() + "\t" + (Integer) entryColumn.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            System.err.println("Usage: UserColumnPv <in_pgv> <in_124> <in_2704> <out> <date> <queue> [number]");
            System.exit(6);
        }

        String inPgv = otherArgs[0];
        String in124 = otherArgs[1];
        String in2704 = otherArgs[2];
        String outDir = otherArgs[3];
        String date = otherArgs[4];
        String queueName = otherArgs[5];
        int reduceNum = 200;
        if (otherArgs.length >= 6) {
            reduceNum = Integer.valueOf(otherArgs[6]).intValue();
        }

        conf.set("mapred.max.map.failures.percent", "1");
        //-Dmapred.job.queue.name=gPgv -Dmapred.queue.name=gPgv
        conf.set("mapred.job.queue.name", queueName);
        conf.set("mapred.queue.name", queueName);
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("mapred.child.java.opts", "-Xmx2700m");
        conf.set("mapred.reduce.max.attempts", "18");

        Job job = new Job(conf, "UserColumnPv");
        job.setJarByClass(UserColumnPv.class);
        job.setMapperClass(SplitMapper.class);
        job.setCombinerClass(SplitCombiner.class);
        job.setReducerClass(SplitReducer.class);
        job.setNumReduceTasks(reduceNum);

        // the map output is Text, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入数据路径
        FileInputFormat.addInputPath(job, new Path(inPgv + "/" + date));
        FileInputFormat.addInputPath(job, new Path(in124 + "/ds=" + date)); // 旧AIO数据
        FileInputFormat.addInputPath(job, new Path(in2704 + "/ds=" + date)); // 新AIO数据

        // 输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(outDir + "/ds=" + date));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}