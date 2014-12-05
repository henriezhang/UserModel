import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class UserColumnSum {

    public static class SplitMapper
            extends Mapper<Object, Text, Text, Text> {
        Pattern p;
        Matcher m;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            p = Pattern.compile("^\\d{5,18}$");
        }


        //100000000       zj:news:news-shwx       1
        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\\t");
            m = p.matcher(fields[0]);
            if (!m.find() || fields.length != 3) {
                return;
            }

            String[] cls = fields[1].split("\\:");
            if (cls.length <= 4) {
                context.write(new Text(fields[0]), new Text("" + cls.length + "\t" + fields[2]));
                context.write(new Text(fields[0]), new Text(fields[1] + "\t" + fields[2]));
            }
        }
    }

    public static class SplitCombiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text Id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map mapColumn = new HashMap();
            for (Text val : values) {
                String[] cls = val.toString().split("\\t");
                Object num = mapColumn.get(cls[0]);
                if (num == null) {
                    mapColumn.put(cls[0], Long.parseLong(cls[1]));
                } else {
                    mapColumn.put(cls[0], (Long) num + Long.parseLong(cls[1]));
                }
            }
            Iterator it = mapColumn.entrySet().iterator();
            while (it.hasNext()) {
                Entry entryColumn = (Entry) it.next();
                context.write(Id, new Text((String) entryColumn.getKey() + "\t" + (Long) entryColumn.getValue()));
            }
        }
    }

    public static class SplitReducer
            extends Reducer<Text, Text, Text, Text> {
        private Map mapId;
        double ratio;
        DecimalFormat df;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            df = new DecimalFormat("#.####");
            mapId = new HashMap();
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                FSDataInputStream in = fs.open(new Path("user_column_count_test/" + context.getConfiguration().get("today") + "/part-r-00000"));
                BufferedReader bufread = new BufferedReader(new InputStreamReader(in));
                String line;
                String[] list;
                while ((line = bufread.readLine()) != null) {
                    list = line.split("\\t");
                    if (list.length < 2) {
                        continue;
                    }
                    mapId.put(list[0], Double.parseDouble(list[1]));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            ratio = Double.parseDouble(context.getConfiguration().get("ratio"));
        }

        @Override
        public void reduce(Text Id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map mapColumn = new HashMap();
            for (Text val : values) {
                String[] cls = val.toString().split("\\t");
                Object num = mapColumn.get(cls[0]);
                if (num == null) {
                    mapColumn.put(cls[0], Long.parseLong(cls[1]));
                } else {
                    mapColumn.put(cls[0], (Long) num + Long.parseLong(cls[1]));
                }
            }

            long[] all = new long[5];
            for (int i = 1; i <= 4; i++) {
                Object num = mapColumn.get("" + i);
                if (num != null) {
                    all[i] = (Long) num;
                    mapColumn.remove("" + i);
                } else {
                    all[i] = 1;
                }
            }
            Iterator it = mapColumn.entrySet().iterator();
            String weight = "";
            while (it.hasNext()) {
                Entry entryColumn = (Entry) it.next();
                String key = (String) entryColumn.getKey();
                String[] cls = key.split("\\:");
                Object num = mapId.get(key);
                if (num != null) {
                    weight += "," + key + ":" + df.format(((Long) entryColumn.getValue() + ratio * (Double) num) / (all[cls.length] + ratio));
                }
            }
            if (weight.length() > 1) {
                context.write(Id, new Text(weight.substring(1)));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: UserColumnSum <in> <out> <startdate> <queue> [number] [days] [ratio]");
            System.exit(4);
        }

        double ratio = 10.0;
        if (otherArgs.length > 6) {
            ratio = Double.parseDouble(otherArgs[6]);
        }

        int reduceNum = 64;
        if (otherArgs.length > 4) {
            reduceNum = Integer.valueOf(otherArgs[4]).intValue();
        }

        int dayNum = 30;
        if (otherArgs.length > 5) {
            dayNum = Integer.valueOf(otherArgs[5]).intValue();
        }

        conf.set("mapred.max.map.failures.percent", "1");
        //-Dmapred.job.queue.name=gPgv -Dmapred.queue.name=gPgv
        conf.set("mapred.job.queue.name", otherArgs[3]);
        conf.set("mapred.queue.name", otherArgs[3]);
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("mapred.child.java.opts", "-Xmx2700m");
        conf.set("mapred.reduce.max.attempts", "18");
        conf.set("today", otherArgs[2]);
        conf.set("ratio", "" + ratio);

        Job job = new Job(conf, "UserColumnSum");
        job.setJarByClass(UserColumnSum.class);
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

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        ParsePosition pos = new ParsePosition(0);
        Date dt = formatter.parse(otherArgs[2], pos);
        Calendar cd = Calendar.getInstance();
        cd.setTime(dt);
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < dayNum; i++) {
            String tmpPath = otherArgs[0] + "/" + formatter.format(cd.getTime());
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd.add(Calendar.DAY_OF_MONTH, 1);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/ds=" + otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
