package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.terracotta.toolkit.Toolkit;
import org.terracotta.toolkit.ToolkitFactory;

import java.io.IOException;
import java.util.Map;

/**
 * hadoop jar target/KDBigMemory-1.0-SNAPSHOT-job.jar /user/mraad_admin/InfoUSA/InfoUSA.txt infousa
 */
public class KDTool extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new KDTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;
        if (args.length != 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            final String worldName = args[1];
            final Toolkit toolkit = ToolkitFactory.createToolkit(KDConst.TOOLKIT_URI_VAL);
            try
            {
                // Clear BigMemory KD Set
                toolkit.getSet(worldName + "Set", KDItem.class).clear();

                // Set BigMemory KD map key/values
                final Map<String, Double> map = toolkit.getMap(worldName + "Map", String.class, Double.class);
                map.put(KDConst.XMIN_KEY, -180.0);
                map.put(KDConst.YMIN_KEY, -90.0);
                map.put(KDConst.XMAX_KEY, 180.0);
                map.put(KDConst.YMAX_KEY, 90.0);
                map.put(KDConst.CELL_KEY, 1.0);

                rc = runJob(args[0], worldName);
            }
            finally
            {
                toolkit.shutdown();
            }
        }
        return rc;
    }

    private int runJob(
            final String path,
            final String worldName) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Configuration configuration = getConf();

        configuration.set(KDConst.KDSET_KEY, worldName + "Set");
        configuration.setFloat(KDConst.XMIN_KEY, -180.0F);
        configuration.setFloat(KDConst.YMIN_KEY, -90.0F);
        configuration.setFloat(KDConst.XMAX_KEY, 180.0F);
        configuration.setFloat(KDConst.YMAX_KEY, 90.0F);
        configuration.setFloat(KDConst.CELL_KEY, 1.0F);

        final Job job = new Job(configuration, KDTool.class.getSimpleName());
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(path));

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(KDMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(KDReducer.class);

        job.setReducerClass(KDReducer.class);

        job.setOutputFormatClass(KDOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
