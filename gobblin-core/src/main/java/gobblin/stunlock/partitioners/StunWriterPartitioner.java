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

import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import gobblin.stunlock.StunUtils;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import gobblin.writer.partitioner.WriterPartitioner;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Strings;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

/**
 * A {@link TimeBasedWriterPartitioner} for {@link GenericRecord}s.
 *
 * The {@link org.apache.avro.Schema.Field} that contains the timestamp can be
 * specified using {@link WRITER_PARTITION_COLUMNS}, and multiple values can be
 * specified, e.g., "header.timestamp,device.timestamp".
 *
 * If multiple values are specified, they will be tried in order. In the above
 * example, if a record contains a valid "header.timestamp" field, its value
 * will be used, otherwise "device.timestamp" will be used.
 *
 * If a record contains none of the specified fields, or if no field is
 * specified, the current timestamp will be used.
 * 
 * 
 * A {@link WriterPartitioner} that partitions a record based on a timestamp.
 *
 * There are two ways to partition a timestamp: (1) specify a
 * {@link DateTimeFormat} using {@link #WRITER_PARTITION_PATTERN}, e.g.,
 * 'yyyy/MM/dd/HH'; (2) specify a {@link TimeBasedWriterPartitioner.Granularity}
 * using {@link #WRITER_PARTITION_GRANULARITY}.
 *
 * A prefix and a suffix can be added to the partition, e.g., the partition path
 * can be 'prefix/2015/11/05/suffix'.
 *
 */
public abstract class StunWriterPartitioner<T> implements WriterPartitioner<T>
{
	private static final Logger LOG = LoggerFactory.getLogger(StunWriterPartitioner.class);

	public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
	public static final String WRITER_PARTITION_PREFIX = ConfigurationKeys.WRITER_PREFIX + ".partition.prefix";
	public static final String WRITER_PARTITION_SUFFIX = ConfigurationKeys.WRITER_PREFIX + ".partition.suffix";
	public static final String WRITER_PARTITION_PATTERN = ConfigurationKeys.WRITER_PREFIX + ".partition.pattern";
	public static final String WRITER_PARTITION_TIMEZONE = ConfigurationKeys.WRITER_PREFIX + ".partition.timezone";
	public static final String DEFAULT_WRITER_PARTITION_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

	public static final String PARTITIONED_RECORD_PARTITIONED_PATH = "partitionedPath";
	public static final String PARTITIONED_RECORD_PREFIX = "prefix";
	public static final String PARTITIONED_RECORD_SUFFIX = "suffix";

	private final String writerPartitionPrefix;
	private final String writerPartitionSuffix;
	private final DateTimeZone timeZone;
	private final Optional<DateTimeFormatter> timestampToPathFormatter;
	private final Schema schema;

	private final Optional<List<String>> partitionColumns;

	private final CachedSchemaRegistryClient schemaRegistry;

	public StunWriterPartitioner(State state)
	{
		this(state, 1, 0);
	}

	public StunWriterPartitioner(State state, int numBranches, int branchId)
	{
		// From TimeBasedWriterPartitioner
		this.writerPartitionPrefix = getWriterPartitionPrefix(state, numBranches, branchId);
		this.writerPartitionSuffix = getWriterPartitionSuffix(state, numBranches, branchId);
		this.timeZone = getTimeZone(state, numBranches, branchId);
		this.timestampToPathFormatter = getTimestampToPathFormatter(state, numBranches, branchId);
		this.schema = generatePartitionSchema();

		// From TimeBasedAvroWriterPartitioner
		this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);

		// New
		this.schemaRegistry = StunUtils.initializeSchemaRegistry(state);
	}

	// ##########################
	// SETTING INITIALIZATION (INVOKED FROM CONSTRUCTOR)
	// ##########################
	private String getWriterPartitionPrefix(State state, int numBranches, int branchId)
	{
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_PREFIX, numBranches, branchId);
		return state.getProp(propName, StringUtils.EMPTY);
	}

	private String getWriterPartitionSuffix(State state, int numBranches, int branchId)
	{
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_SUFFIX, numBranches, branchId);
		return state.getProp(propName, StringUtils.EMPTY);
	}

	private Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId)
	{
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
		return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
	}

	private Optional<DateTimeFormatter> getTimestampToPathFormatter(State state, int numBranches, int branchId)
	{
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_PATTERN, numBranches, branchId);

		if (state.contains(propName))
		{
			return Optional.of(DateTimeFormat.forPattern(state.getProp(propName)).withZone(this.timeZone));
		}
		else
		{
			LOG.error("StunWriterPartitioner: No Timestamp Schema Present. Probably missing in configuration.");
			return Optional.absent();
		}
	}

	private DateTimeZone getTimeZone(State state, int numBranches, int branchId)
	{
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_TIMEZONE, numBranches, branchId);
		return DateTimeZone.forID(state.getProp(propName, DEFAULT_WRITER_PARTITION_TIMEZONE));
	}

	private Schema generatePartitionSchema()
	{
		if (this.timestampToPathFormatter.isPresent() == false)
		{
			LOG.error("StunWriterPartitioner: No Timestamp Schema Present. Probably missing in configuration.");
			return null;
		}

		FieldAssembler<Schema> assembler = SchemaBuilder.record("GenericRecordTimePartition").namespace("gobblin.writer.partitioner").fields();

		if (!Strings.isNullOrEmpty(this.writerPartitionPrefix))
		{
			assembler = assembler.name(PARTITIONED_RECORD_PREFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
		}
		assembler = assembler.name(PARTITIONED_RECORD_PARTITIONED_PATH).type(Schema.create(Schema.Type.STRING)).noDefault();
		if (!Strings.isNullOrEmpty(this.writerPartitionSuffix))
		{
			assembler = assembler.name(PARTITIONED_RECORD_SUFFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
		}

		return assembler.endRecord();
	}

	// #######################################################################
	// ACTUAL IMPLEMENTATION OF WriterPartitioner. Entry Point for Partitioning.
	// #######################################################################

	@Override
	public Schema partitionSchema()
	{
		return this.schema;
	}

	// ENTRYPOINT
	@Override
	public abstract GenericRecord partitionForRecord(T record);

	public GenericRecord partitionForGenericRecord(GenericRecord avroRecord)
	{
		Schema recordSchema = avroRecord.getSchema();
		int recordSchemaId = 0;
		try
		{
			recordSchemaId = StunUtils.getSchemaId(schemaRegistry, recordSchema);
		}
		catch (IOException e)
		{
			LOG.error("StunWriterPartitioner: IOException in SchemaRegistry. " + e.toString());
			return null;
		}
		catch (RestClientException e)
		{
			LOG.error("StunWriterPartitioner: RestClientException in SchemaRegistry. " + e.toString());
			return null;
		}

		long timestamp = StunUtils.getRecordTimestamp(this.partitionColumns, avroRecord);
		GenericRecord partition = new GenericData.Record(this.schema);
		if (!Strings.isNullOrEmpty(this.writerPartitionPrefix))
		{
			partition.put(PARTITIONED_RECORD_PREFIX, this.writerPartitionPrefix);
		}
		if (!Strings.isNullOrEmpty(this.writerPartitionSuffix))
		{
			partition.put(PARTITIONED_RECORD_SUFFIX, this.writerPartitionSuffix);
		}

		if (this.timestampToPathFormatter.isPresent())
		{
			String partitionedPath = recordSchema.getName() + "_" + recordSchemaId + Path.SEPARATOR + getPartitionedPath(timestamp);
			partition.put(PARTITIONED_RECORD_PARTITIONED_PATH, partitionedPath);
		}
		else
		{
			LOG.error("StunWriterPartitioner: No Timestamp Schema Present. Probably missing in configuration.");
			return null;
		}

		return partition;
	}

	private String getPartitionedPath(long timestamp)
	{
		return this.timestampToPathFormatter.get().print(timestamp);
	}
}