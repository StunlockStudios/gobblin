package gobblin.stunlock;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class ConfluentSchemaRegistry extends KafkaSchemaRegistry<Integer, Schema>
{
	private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaRegistry.class);
	private final CachedSchemaRegistryClient schemaRegistry;

	public static final String SCHEMA_REGISTRY_MAX_CACHE_SIZE = "kafka.schema.registry.max.cache.size";
	public static final String SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
	public static final String DEFAULT_SCHEMA_REGISTRY_MAX_CACHE_SIZE = "1000";

	public ConfluentSchemaRegistry(Properties properties)
	{
		super(properties);

		String url = properties.getProperty(SCHEMA_REGISTRY_URL);
		int maxCacheSize = Integer.parseInt(properties.getProperty(SCHEMA_REGISTRY_MAX_CACHE_SIZE, DEFAULT_SCHEMA_REGISTRY_MAX_CACHE_SIZE));
		this.schemaRegistry = new CachedSchemaRegistryClient(url, maxCacheSize);
	}

	@Override
	protected Schema fetchSchemaByKey(Integer key) throws SchemaRegistryException
	{
		try
		{
			return this.schemaRegistry.getByID(key);
		}
		catch (IOException e)
		{
			LOG.error("fetchSchemaByKey(" + key + ") - Got exception", e);
			throw new SchemaRegistryException(e);
		}
		catch (RestClientException e)
		{
			LOG.error("fetchSchemaByKey(" + key + ") - Got exception", e);
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException
	{
		try
		{
			return new Schema.Parser().parse(this.schemaRegistry.getLatestSchemaMetadata(topic).getSchema());
		}
		catch (IOException e)
		{
			LOG.error("getLatestSchemaByTopic(" + topic + ") - Got exception", e);
			throw new SchemaRegistryException(e);
		}
		catch (RestClientException e)
		{
			LOG.error("getLatestSchemaByTopic(" + topic + ") - Got exception", e);
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public Integer register(Schema schema) throws SchemaRegistryException
	{
		try
		{
			return this.schemaRegistry.register(schema.getFullName(), schema);
		}
		catch (IOException e)
		{
			LOG.error("register (1 param) - Got exception", e);
			throw new SchemaRegistryException(e);
		}
		catch (RestClientException e)
		{
			LOG.error("register (1 param) - Got exception", e);
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public Integer register(Schema schema, String name) throws SchemaRegistryException
	{
		try
		{
			return this.schemaRegistry.register(name, schema);
		}
		catch (IOException e)
		{
			LOG.error("register (2 param) - Got exception", e);
			throw new SchemaRegistryException(e);
		}
		catch (RestClientException e)
		{
			LOG.error("register (2 param) - Got exception", e);
			throw new SchemaRegistryException(e);
		}
	}

}
