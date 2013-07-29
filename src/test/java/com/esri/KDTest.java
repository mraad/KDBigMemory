package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class KDTest
{
    private MapDriver<
            LongWritable, Text,
            LongWritable, IntWritable
            > m_mapDriver;
    private ReduceDriver<
            LongWritable, IntWritable,
            LongWritable, IntWritable
            > m_reduceDriver;
    private MapReduceDriver<
            LongWritable, Text,
            LongWritable, IntWritable,
            LongWritable, IntWritable
            > m_mapReduceDriver;

    private Configuration m_configuration;

    private KDMapper m_mapper;

    @Before
    public void setUp()
    {
        m_configuration = new Configuration();

        m_configuration.setFloat(KDConst.XMIN_KEY, -180);
        m_configuration.setFloat(KDConst.YMIN_KEY, -90);
        m_configuration.setFloat(KDConst.XMAX_KEY, 180);
        m_configuration.setFloat(KDConst.YMAX_KEY, 90);
        m_configuration.setFloat(KDConst.CELL_KEY, 1);

        m_mapper = new KDMapper();

        m_mapDriver = new MapDriver<LongWritable, Text, LongWritable, IntWritable>();
        m_mapDriver.setMapper(m_mapper);
        m_mapDriver.setConfiguration(m_configuration);

        final KDReducer reducer = new KDReducer();

        m_reduceDriver = new ReduceDriver<LongWritable, IntWritable, LongWritable, IntWritable>();
        m_reduceDriver.setReducer(reducer);
        m_reduceDriver.setConfiguration(m_configuration);

        m_mapReduceDriver = new MapReduceDriver<
                LongWritable, Text,
                LongWritable, IntWritable,
                LongWritable, IntWritable>();
        m_mapReduceDriver.setMapper(m_mapper);
        m_mapReduceDriver.setReducer(reducer);
        m_mapReduceDriver.setConfiguration(m_configuration);
    }

    @Test
    public void testMapper() throws IOException
    {
        final double lon = -180.0;
        final double lat = -90.0;
        m_mapDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join("\t", Arrays.asList(
                "ID", Double.toString(lat), Double.toString(lon)))));
        m_mapDriver.resetOutput();
        m_mapDriver.addOutput(new LongWritable(GeoHash.geohash(lon, lat, -180, -90, 1)), KDMapper.ONE);
        m_mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException
    {
        final List<IntWritable> list = new ArrayList<IntWritable>();
        list.add(KDMapper.ONE);
        list.add(KDMapper.ONE);
        list.add(KDMapper.ONE);

        final LongWritable rowcol = new LongWritable();
        m_reduceDriver.withInput(rowcol, list);
        m_reduceDriver.withOutput(rowcol, new IntWritable(3));
        m_reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException
    {
        final double lon = 180.0;
        final double lat = 90.0;
        m_mapReduceDriver.withInput(
                new LongWritable(0), new Text(StringUtils.join("\t", Arrays.asList(
                "ID", Double.toString(lat), Double.toString(lon)))));
        m_mapReduceDriver.resetOutput();
        m_mapReduceDriver.addOutput(new LongWritable(GeoHash.geohash(lon, lat, -180, -90, 1)), KDMapper.ONE);
        m_mapReduceDriver.runTest();
    }

}
