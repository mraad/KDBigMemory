package com.esri;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.terracotta.toolkit.Toolkit;
import org.terracotta.toolkit.ToolkitFactory;
import org.terracotta.toolkit.ToolkitInstantiationException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 */
public final class KDRecordWriter extends RecordWriter<LongWritable, IntWritable>
{
    private final static Log m_log = LogFactory.getLog(KDRecordWriter.class);

    private Toolkit m_toolkit;
    private Set<KDItem> m_set;

    private final class NoopSet implements Set<KDItem>
    {
        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public boolean contains(final Object o)
        {
            return false;
        }

        @Override
        public Iterator iterator()
        {
            return null;
        }

        @Override
        public KDItem[] toArray()
        {
            return new KDItem[0];
        }

        @Override
        public boolean add(final KDItem kdItem)
        {
            return false;
        }

        @Override
        public Object[] toArray(final Object[] objects)
        {
            return new KDItem[0];
        }

        @Override
        public boolean remove(final Object o)
        {
            return false;
        }

        @Override
        public boolean containsAll(final Collection<?> objects)
        {
            return false;
        }

        @Override
        public boolean addAll(final Collection collection)
        {
            return false;
        }

        @Override
        public boolean retainAll(final Collection<?> objects)
        {
            return false;
        }

        @Override
        public boolean removeAll(final Collection<?> objects)
        {
            return false;
        }

        @Override
        public void clear()
        {
        }
    }

    public KDRecordWriter(final Configuration configuration)
    {
        try
        {
            m_toolkit = ToolkitFactory.createToolkit(
                    configuration.get(KDConst.TOOLKIT_URI_KEY, KDConst.TOOLKIT_URI_VAL));
            m_set = m_toolkit.getSet(
                    configuration.get(KDConst.KDSET_KEY, KDConst.KDSET_VAL), KDItem.class);
        }
        catch (ToolkitInstantiationException e)
        {
            m_log.error(e.toString(), e);
            m_set = new NoopSet();
        }
    }

    @Override
    public void write(
            final LongWritable key,
            final IntWritable val
    ) throws IOException, InterruptedException
    {
        m_set.add(new KDItem(key.get(), val.get()));
    }

    @Override
    public void close(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        if (m_toolkit != null)
        {
            m_toolkit.shutdown();
        }
    }
}
