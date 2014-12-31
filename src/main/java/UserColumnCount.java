
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * record sample
 * 100000000       zj:news:news-shwx       1
 * <p/>
 * <p/>
 * 100000000       zj:news:news-shwx       1
 * 100000000       zj:news 1
 * 100000000       zj      1
 * 100000488       zj:news 1
 * 100000488       zj      1
 * 100001180       news    1
 * 100001180       news:newssh     1
 * 100001180       news:newssh:shwx        1
 * 100002314       astro   11
 * 100002314       astro:kuaixun   11
 * 100003575       ent:star:star-news      1
 * 100003575       ent:star:star-news:dalustar     1
 * 100003575       ent:star        1
 * 100003575       ent     1
 * 100005320       ent:star:star-news:gangtaistar  1
 * 100005320       news    2
 * 100005320       news:photon:phshis      2
 * 100005320       ent:star:star-news      2
 * 100005320       ent:star:star-news:dalustar     1
 * 100005320       ent:star        2
 * 100005320       news:photon     2
 * 100005320       ent     2
 * 100011918       news    3
 * 100011918       news:newssh     1
 * 100011918       news:newssh:shwx        1
 * 100011918       sports  2
 * 100011918       sports:photo    1
 * 100011918       news:photon     2
 * 100011918       sports:csocce:jiaa:gzhd 1
 * 100011918       sports:csocce:jiaa      1
 * 100011918       sports:csocce   1
 * 100011918       news:photon:gjshtp      2
 * 10001265        sh:news:mssh    1
 * 10001265        news    2
 * 10001265        sh:edu:college  1
 * 10001265        news:newsgn:zhxw        1
 * 10001265        news:newssh     1
 * 10001265        news:newssh:shwx        1
 * 10001265        sh:news 1
 * 10001265        news:newsgn     1
 * 10001265        sh:edu  2
 * 10001265        sh      3
 * 10001265        sh:edu:edunews  1
 * 10001346        digi:shumayaowen:shoujiyaowen   1
 * 10001346        digi    1
 * 10001346        digi:shumayaowen        1
 * 100013619       hn:news 1
 * 100013619       hn:news:sz      1
 * 100013619       hn      1
 * 100018326       hn:news 1
 * 100018326       hn:news:sz      1
 * 100018326       hn      1
 * 100023096       sh:news:mssh    1
 * 100023096       sh:news 1
 * 100023096       sh      1
 * 1000312         1
 * 1000312 fashion 3
 * 1000312 sports  3
 * 1000312 news:newsgj     3
 * 1000312 sports:csocce:jiaa      1
 * 1000312 news:newsgj:rdzh        2
 * 1000312 finance:financenews:domestic    1
 * 1000312 ent:tv:zy:zyw   1
 * 1000312 sports:others   2
 * 1000312 sports:others:swimin:dyynews    1
 * 1000312 news:newsgn:zhxw        4
 * 1000312 sports:csocce:jiaa:cajing       1
 * 1000312 fashion:beauty  3
 * 1000312 news:newsgn:gdxw        4
 * 1000312 news    11
 * 1000312 ent:star:star-news:gangtaistar  1
 * 1000312 ent:tv:zy       1
 * 1000312 finance:financenews:international       1
 * 1000312 sports:others:swimin    1
 * 1000312 ent     4
 * 1000312 sports:csocce   1
 * 1000312 ent:tv  1
 * 1000312 sports:others:tableb:snookernews        1
 * 1000312 ent:star:star-news      3
 * 1000312 ent:star:star-news:dalustar     2
 * 1000312 finance:financenews     2
 * 1000312 news:newsgn     8
 * 1000312 finance 2
 * 1000312 ent:star        3
 * 1000312 sports:others:tableb    1
 * 100032168       jiangsu 1
 * 100032168       jiangsu:news:shehui     1
 * 100032168       jiangsu:news    1
 * 10005243        news    4
 * 10005243        news:newssh     4
 * 10005243        news:newssh:shwx        4
 * 100057828       zj:news:news-shwx       1
 * 100057828       zj:news 2
 * 100057828       zj      2
 * 100057909       zj:news 1
 * 100057909       zj:article-qq   1
 * 100057909       zj      2
 * 100059899       gd:news:headpic 1
 * 100059899       news    1
 * 100059899       news:newssh     1
 */
public class UserColumnCount {

    public static class SplitMapper extends Mapper<Object, Text, Text, Text> {
        Pattern p;
        Matcher m;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            p = Pattern.compile("^\\d{5,18}$");
        }

        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {

            String[] fields = inValue.toString().split("\\t");
            m = p.matcher(fields[0]);
            //check whether the first filed is id(number of qq)
            if (!m.find() || fields.length != 3) {
                return;
            }

            String[] cls = fields[1].split("\\:");
            if (cls.length == 1) {
                context.write(new Text("" + cls.length), new Text(fields[2]));
            }
            context.write(new Text(fields[1]), new Text(fields[2]));
        }
    }

    public static class SplitCombiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text Id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (Text val : values) {
                try {
                    sum += Long.parseLong(val.toString());
                } catch (Exception e) {
                    continue;
                }
            }
            context.write(new Text(""), new Text(Id.toString() + "\t" + sum));
        }
    }

    public static class SplitReducer
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
            while (it.hasNext()) {
                Entry entryColumn = (Entry) it.next();
                String key = (String) entryColumn.getKey();
                Long value = (Long) entryColumn.getValue();
                if (value < 100) {
                    continue;
                }
                String[] cls = key.split("\\:");
                int pos = key.lastIndexOf(":");
                if (pos >= 0) {
                    Object num = mapColumn.get(key.substring(0, pos));
                    if (num != null) {
                        context.write(new Text(key), new Text("" + (value + 0.0) / (Long) num));
                    }
                } else {
                    context.write(new Text(key), new Text("" + (value + 0.0) / all[1]));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: UserColumnCount <in> <out> <startdate> <queue> [number] [days]");
            System.exit(4);
        }

        int reduceNum = 64;
        if (otherArgs.length > 4) {
            reduceNum = Integer.valueOf(otherArgs[4]);
        }

        int dayNum = 30;
        if (otherArgs.length > 5) {
            dayNum = Integer.valueOf(otherArgs[5]);
        }

        conf.set("mapred.max.map.failures.percent", "1");
        //-Dmapred.job.queue.name=gPgv -Dmapred.queue.name=gPgv
        conf.set("mapred.job.queue.name", otherArgs[3]);
        conf.set("mapred.queue.name", otherArgs[3]);
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("mapred.child.java.opts", "-Xmx2700m");
        conf.set("mapred.reduce.max.attempts", "18");

        Job job = new Job(conf, "UserColumnCount");
        job.setJarByClass(UserColumnCount.class);
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
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/" + otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
