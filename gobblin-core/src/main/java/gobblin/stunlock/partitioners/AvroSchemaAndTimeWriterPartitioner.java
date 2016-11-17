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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import gobblin.writer.partitioner.WriterPartitioner;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

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
public class AvroSchemaAndTimeWriterPartitioner extends StunWriterPartitioner<GenericRecord> {

	public AvroSchemaAndTimeWriterPartitioner(State state) {
		super(state);
	}

	public AvroSchemaAndTimeWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	// Kept this class just so that we can use it in the old jobs with no issue.
	@Override
	public GenericRecord partitionForRecord(GenericRecord record) {
		return super.partitionForGenericRecord(record);
	}
}