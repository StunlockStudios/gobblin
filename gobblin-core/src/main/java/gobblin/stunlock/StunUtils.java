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

public class StunUtils {
	public static CachedSchemaRegistryClient initializeSchemaRegistry(State state) {
		Properties properties = state.getProperties();
		String url = properties.getProperty(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);

		int maxCacheSize = Integer
				.parseInt(properties.getProperty(ConfluentSchemaRegistry.SCHEMA_REGISTRY_MAX_CACHE_SIZE,
						ConfluentSchemaRegistry.DEFAULT_SCHEMA_REGISTRY_MAX_CACHE_SIZE));
		return new CachedSchemaRegistryClient(url, maxCacheSize);
	}

	public static int getSchemaId(CachedSchemaRegistryClient schemaRegistry, Schema recordSchema) throws IOException, RestClientException {
		// LOG.info("PARTITIONER GetSchemaID: Get for schema with name: " +
		// recordSchema.getName());
		int schemaId = schemaRegistry.register(recordSchema.getName(), recordSchema);
		// LOG.info("PARTITIONER GetSchemaID: Result schema Id: " + schemaId);
		return schemaId;
	}

	/**
	 * From TimeBasedAvroWriterPartitioner. I compressed the code a bit so it's
	 * eligible. Check if the partition column value is present and is a Long
	 * object. Otherwise, use current system time.
	 */
	public static long getRecordTimestamp(Optional<List<String>> partitionColumns, GenericRecord record) {
		if (partitionColumns.isPresent() == false) {
			return System.currentTimeMillis(); // Default Value
		}

		for (String partitionColumn : partitionColumns.get()) {
			Optional<Object> fieldValue = AvroUtils.getFieldValue(record, partitionColumn);
			if (fieldValue.isPresent() && fieldValue.orNull() instanceof Long) {
				return (Long) fieldValue.get(); // Get real value
			}
		}
		return System.currentTimeMillis(); // Default Value
	}

	public static String getSchemaRegistryBaseUrl(State state) {
		return state.getProp(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);
	}
	
	public static String getSchemaUrl(State state, int schemaId) {
		return getSchemaRegistryBaseUrl(state) + "/schemas/ids/plain/" + schemaId;
	}
}