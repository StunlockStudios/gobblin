/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.stunlock;

import gobblin.writer.FsDataWriter;
import java.io.IOException;
import java.lang.System;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.configuration.State;

/**
 * An extension to {@link FsDataWriter} that writes {@link Writable} records
 * using an {@link org.apache.hadoop.mapred.OutputFormat} that implements
 * {@link HiveOutputFormat}.
 *
 * The records are written using a {@link RecordWriter} created by
 * {@link HiveOutputFormat#getHiveRecordWriter(JobConf, org.apache.hadoop.fs.Path, Class, boolean, java.util.Properties, org.apache.hadoop.util.Progressable)}.
 *
 * @author ziliu
 */
public class AvroToOrcDataWriter extends FsDataWriter<AvroGenericRecordWritable> {
	protected RecordWriter writer;
	protected final AtomicLong count = new AtomicLong(0);
	private Throwable _WriterThrow;
	private int _WriteCount = 0;
	private long _StartTime;
	
	private AvroToOrcConverter converter; // Have to convert this last step locally as gobblin sucks.
	
	private static final Logger LOG = LoggerFactory.getLogger(AvroToOrcDataWriter.class);

	public AvroToOrcDataWriter(AvroToOrcDataWriterBuilder<?> builder, State properties) throws IOException {
	    super(builder, properties);
		_StartTime = System.currentTimeMillis();

	    Preconditions.checkArgument(this.properties.contains(AvroToOrcDataWriterBuilder.WRITER_OUTPUT_FORMAT_CLASS));
		this.writer = getWriter();
		this.converter = new AvroToOrcConverter();
		this.converter.init(properties);
	  }

	private RecordWriter getWriter() throws IOException {
		try {
			HiveOutputFormat<?, ?> outputFormat = HiveOutputFormat.class.cast(
					Class.forName(this.properties.getProp(AvroToOrcDataWriterBuilder.WRITER_OUTPUT_FORMAT_CLASS))
							.newInstance());

			@SuppressWarnings("unchecked")
			Class<? extends Writable> writableClass = (Class<? extends Writable>) Class
					.forName(this.properties.getProp(AvroToOrcDataWriterBuilder.WRITER_WRITABLE_CLASS));

			return outputFormat.getHiveRecordWriter(new JobConf(), this.stagingFile, writableClass, true, this.properties.getProperties(), null);
		} catch (Throwable t) {
			LOG.error("Creating writer threw excpetion: " + t);
			throw new IOException(String.format("Failed to create writer"), t);
		}
	}

	@Override
	public void write(AvroGenericRecordWritable record) throws IOException {
		Preconditions.checkNotNull(record);
		
		Writable orcRecord = null;
		try
		{
			orcRecord = this.converter.convertRecord(record);

			if(orcRecord == null)
				throw new IOException("Failed to convert? Returned OrcRecord was null!");
		}
		catch (Throwable t)
		{
			LOG.error("ERROR converting record to " + this.stagingFile + ". Write threw excpetion. ");
			LOG.error("" + t);
			throw new IOException(t);
		}

		_WriteCount++;
		try
		{
			this.writer.write(orcRecord);
		}
		catch (Throwable t)
		{
			LOG.error("ERROR writing record to " + this.stagingFile + ". Write threw excpetion. ");
			LOG.error("" + t);
			throw (t);
		}
		this.count.incrementAndGet();
	}

	@Override
	public long recordsWritten() {
		return this.count.get();
	}

	@Override
	public long bytesWritten() throws IOException {
		if (!this.fs.exists(this.outputFile)) {
			return 0;
		}

		return this.fs.getFileStatus(this.outputFile).getLen();
	}
	
	@Override
	public void postProcessRecords() throws IOException {
		try
		{
			long endTime = System.currentTimeMillis();
			long duration = endTime - _StartTime;
			LOG.info("Closing writer after writing " + _WriteCount + " records over " + (duration / 1000.0) +  " seconds . File: " + this.fileName);
			this.writer.close(false);
		}
		catch(Throwable t)
		{
			LOG.error("Closing writer of " + this.stagingFile + " threw excpetion. " + t);
			_WriterThrow = t; // Store to throw on commit instead.
		}
		super.postProcessRecords();
	}


	@Override
	public void commit() throws IOException {
		if(_WriterThrow != null) {
			LOG.error("Throwing prethrown _WriterThrow on commit for " + this.stagingFile + ".");
			throw new IOException("Prethrown write.close error", _WriterThrow);
		}
		super.commit();
	}

}
