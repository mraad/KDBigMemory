package com.esri;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 */
public final class KDOutputFormat extends OutputFormat
{
    @Override
    public RecordWriter getRecordWriter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new KDRecordWriter(taskAttemptContext.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(final JobContext jobContext) throws IOException, InterruptedException
    {
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new OutputCommitter()
        {
            @Override
            public void setupJob(final JobContext jobContext) throws IOException
            {
            }

            @Override
            public void setupTask(final TaskAttemptContext taskAttemptContext) throws IOException
            {
            }

            @Override
            public boolean needsTaskCommit(final TaskAttemptContext taskAttemptContext) throws IOException
            {
                return false;
            }

            @Override
            public void commitTask(final TaskAttemptContext taskAttemptContext) throws IOException
            {
            }

            @Override
            public void abortTask(final TaskAttemptContext taskAttemptContext) throws IOException
            {
            }
        };
    }
}
