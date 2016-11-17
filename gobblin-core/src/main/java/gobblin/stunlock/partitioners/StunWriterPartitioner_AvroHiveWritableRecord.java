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

public class StunWriterPartitioner_AvroHiveWritableRecord extends StunWriterPartitioner<AvroHiveWritableRecord> {

	public StunWriterPartitioner_AvroHiveWritableRecord(State state) {
		super(state);
	}

	public StunWriterPartitioner_AvroHiveWritableRecord(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	// ENTRYPOINT
	@Override
	public GenericRecord partitionForRecord(AvroHiveWritableRecord record) {
		return super.partitionForGenericRecord(record.avroSourceRecord);
	}
}