package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class KDMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>
{
    private final Pattern m_pattern = Pattern.compile("^.+\\t(-?\\d+\\.\\d+)\\t(-?\\d+\\.\\d+)$");

    public final static IntWritable ONE = new IntWritable(1);

    private final LongWritable m_key = new LongWritable();

    private double m_xmin;
    private double m_ymin;
    private double m_xmax;
    private double m_ymax;
    private double m_fact;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();
        m_xmin = configuration.getFloat(KDConst.XMIN_KEY, -180.0F);
        m_ymin = configuration.getFloat(KDConst.YMIN_KEY, -90.0F);
        m_xmax = configuration.getFloat(KDConst.XMAX_KEY, 180.0F);
        m_ymax = configuration.getFloat(KDConst.YMAX_KEY, 90.0F);
        m_fact = 1.0 / configuration.getFloat(KDConst.CELL_KEY, 1.0F);
    }

    public void map(
            final LongWritable lineno,
            final Text line,
            final Context context
    ) throws IOException, InterruptedException
    {
        final Matcher matcher = m_pattern.matcher(line.toString());
        if (matcher.matches())
        {
            final double y = Double.parseDouble(matcher.group(1));
            if (y < m_ymin)
            {
                return;
            }
            if (y > m_ymax)
            {
                return;
            }
            final double x = Double.parseDouble(matcher.group(2));
            if (x < m_xmin)
            {
                return;
            }
            if (x > m_xmax)
            {
                return;
            }
            m_key.set(GeoHash.geohash(x, y, m_xmin, m_ymin, m_fact));
            context.write(m_key, ONE);
        }
    }

}