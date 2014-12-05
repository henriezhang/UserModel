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
            p = Pattern.compile("^\\d{5,18}$");
        }

        @Override
        public void map(Object key, Text inValue, Context context) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\\|");
            if (fields.length < 17) {
                return;
            }

            m = p.matcher(fields[16]);
            if (!m.find()) {
                return;
            }
            int pos1 = fields[9].indexOf("L.");
            if (fields[16].length() >= 5) {

                if (pos1 >= 0) {
                    int pos2 = fields[9].indexOf("_", pos1);
                    if (pos2 > pos1) {

                        String column = fields[9].substring(pos1 + 2, pos2);

                        if (column.startsWith("news.milite")) {
                            String strValue = "milite" + "|" + fields[3] + fields[4];
                            String normalizedValue = strValue.replaceAll("-", "_");
                            context.write(new Text(fields[16]), new Text(normalizedValue));
                        } else if (column.startsWith("news.newssh")) {
                            String strValue = "society" + "|" + fields[3] + fields[4];
                            String normalizedValue = strValue.replaceAll("-", "_");
                            context.write(new Text(fields[16]), new Text(normalizedValue));
                        } else {
                            String[] cls = column.split("\\.");

                            if (cls.length > 0) {
                                //just keep site, don't take into account all columns
                                String site = cls[0].trim();
                                String strValue = site + "|" + fields[3] + fields[4];
                                String normalizedValue = strValue.replaceAll("-", "_");
                                context.write(new Text(fields[16]), new Text(normalizedValue));
                            }

//                            for (int i = 0; i < cls.length; i++)
//                            {
//                                String item = cls[0].trim();
//                                for (int j = 1; j <= i; j++)
//                                {
//                                    item += ":" + cls[j].trim();
//                                }
//                                String strValue = item + "|" + fields[3] + fields[4];
//                                String normalizedValue = strValue.replaceAll("-", "_");
//                                context.write(new Text(fields[16]), new Text(normalizedValue));
//                            }
                        }
                    }
                } else {
                    String curDomain = fields[3];
                    if (curDomain.equals("film.qq.com") || curDomain.equals("v.qq.com")) {
                        String strValue = "v" + "|" + fields[3] + fields[4];
                        String normalizedValue = strValue.replaceAll("-", "_");
                        context.write(new Text(fields[16]), new Text(normalizedValue));
                    } else if (curDomain.equals("dajia.qq.com")) {
                        String strValue = "dajia" + "|" + fields[3] + fields[4];
                        String normalizedValue = strValue.replaceAll("-", "_");
                        context.write(new Text(fields[16]), new Text(normalizedValue));
                    }
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
        if (otherArgs.length < 4) {
            System.err.println("Usage: UserColumnPv <in> <out> <date> <queue> [number]");
            System.exit(4);
        }

        int reduceNum = 64;
        if (otherArgs.length > 4) {
            reduceNum = Integer.valueOf(otherArgs[4]).intValue();
        }

        conf.set("mapred.max.map.failures.percent", "1");
        //-Dmapred.job.queue.name=gPgv -Dmapred.queue.name=gPgv
        conf.set("mapred.job.queue.name", otherArgs[3]);
        conf.set("mapred.queue.name", otherArgs[3]);
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

//        for (int i = 0; i < 24; i++)
//        {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/" + otherArgs[2] + "/" + String.format("%02d", i)));
//        }

        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/" + otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/" + otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
