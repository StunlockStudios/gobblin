package gobblin.stunlock.schemaflattening;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.stunlock.ConfluentSchemaRegistry;
import gobblin.stunlock.StunlockKafkaAvroExtractor;
import gobblin.util.ForkOperatorUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class FlattenConverter extends AvroToAvroConverterBase
{
	private static final Logger LOG = LoggerFactory.getLogger(FlattenConverter.class);

	@Override
	public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException
	{
		String arrayNameProp = getForkedPropName(workUnit, FlattenForkOperator.FLATTEN_ARRAY_NAME);
		String outputSchemaNameProp = getForkedPropName(workUnit, FlattenForkOperator.FLATTEN_ARRAY_OUTPUTSCHEMA);
		
		if (workUnit.contains(arrayNameProp) == false)
			throw new SchemaConversionException("EXPECTED ARRAY NAME PROPERTY '" + arrayNameProp + "' NOT SET!");
		if (workUnit.contains(outputSchemaNameProp) == false)
			throw new SchemaConversionException("EXPECTED FLATTENED OUTPUT SCHEMA NAME PROPERTY '" + outputSchemaNameProp + "' NOT SET!");

		String arrayName = workUnit.getProp(arrayNameProp);
		String outputSchemaName = workUnit.getProp(outputSchemaNameProp);
		try
		{
			FlattenedSchema schema = SchemaFlattener.getFlattenedSchema(inputSchema, arrayName, outputSchemaName);
			TESTLOG("SCHEMA FLATTENED FOR ARRAY " + arrayName + ". REGISTERING SCHEMA.");

			// REGISTER THE SCHEMA!
			int schemaID = RegisterSchema(workUnit, schema.Schema);
			TESTLOG("SCHEMA REGISTERED WITH ID " + schemaID);
			return schema.Schema;
		}
		catch (Exception e)
		{
			throw new SchemaConversionException(e);
		}
	}

	private static String getForkedPropName(WorkUnitState workUnit, String propName)
	{
		int forkIndex = workUnit.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY, 0);
		return propName + "." + forkIndex;
	}

	private int RegisterSchema(WorkUnitState workUnit, Schema schema) throws IOException, RestClientException
	{
		Properties properties = workUnit.getProperties();
		String url = properties.getProperty(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);
		int maxCacheSize = Integer.parseInt(properties.getProperty(ConfluentSchemaRegistry.SCHEMA_REGISTRY_MAX_CACHE_SIZE, ConfluentSchemaRegistry.DEFAULT_SCHEMA_REGISTRY_MAX_CACHE_SIZE));

		CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(url, maxCacheSize);
		return schemaRegistry.register(schema.getFullName(), schema);
	}

	private void TESTLOG(String text)
	{
		// LOG.error(text);
	}

	@Override
	public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
			throws DataConversionException
	{
		List<GenericRecord> outputRecords = new ArrayList<GenericRecord>();
		TESTLOG("----");
		TESTLOG("CONVERTING RECORD!");
		TESTLOG("Input Schema: \n" + inputRecord.getSchema().toString(true));
		TESTLOG("----");
		TESTLOG("Output Schema: \n" + outputSchema.toString(true));
		TESTLOG("----");
		TESTLOG("Input Record: \n" + inputRecord.toString());
		TESTLOG("----");

		String arrayNameProp = getForkedPropName(workUnit, FlattenForkOperator.FLATTEN_ARRAY_NAME);
		if (workUnit.contains(arrayNameProp) == false)
		{
			throw new DataConversionException("EXPECTED ARRAY NAME PROPERTY '" + arrayNameProp + "' NOT SET!");
		}
		String arrayName = workUnit.getProp(arrayNameProp);
		TESTLOG("TARGET ARRAY " + arrayName + ". LETS FIND IT");

		Object targetArray = inputRecord.get(arrayName);
		TESTLOG("ARRAY GET DONE: " + targetArray.getClass().getName());

		@SuppressWarnings("unchecked")
		GenericData.Array<GenericData.Record> array = (GenericData.Array<GenericData.Record>) targetArray;
		TESTLOG("CAST DONE. SIZE: " + array.size());

		List<String> addedFields = new ArrayList<String>();

		for (GenericData.Record record : array)
		{
			addedFields.clear();

			GenericRecord newRecord = new GenericData.Record(outputSchema);

			TESTLOG("RECORD: " + record.toString());
			TESTLOG("SCHEMA: " + record.getSchema().toString(true));
			for (Field field : record.getSchema().getFields())
			{
				String fieldName = field.name();
				Object fieldValue = record.get(fieldName);

				String outputFieldName = arrayName + "_" + fieldName;
				newRecord.put(outputFieldName, fieldValue);
				addedFields.add(outputFieldName);
			}

			TESTLOG("LOOP FIELDS");
			// TODO: LOOP BASE FIELDS (IGNORE ALL ARRAYS) AND INSERT INTO
			// newRecord
			for (Field outputField : outputSchema.getFields())
			{
				TESTLOG("FIELDS " + outputField.name() + ", addedFields.size " + addedFields.size());
				if (addedFields.contains(outputField.name()) == false)
				{
					TESTLOG("BASIC NAME: " + outputField.name() + ", NOT ADDED!");
					Object value = inputRecord.get(outputField.name());
					TESTLOG("VALUE:" + value);
					newRecord.put(outputField.name(), value);
					addedFields.add(outputField.name());
				}
			}
			TESTLOG("LOOP FIELDS DONE");

			// TODO: ADD newRecord TO RESULT ARRAY
			outputRecords.add(newRecord);
		}

		TESTLOG("RECORDS CONVERTED!");
		for (GenericRecord outputRecord : outputRecords)
		{
			TESTLOG(outputRecord.toString());
		}
		TESTLOG("RECORDS END!");
		return outputRecords;
	}
}