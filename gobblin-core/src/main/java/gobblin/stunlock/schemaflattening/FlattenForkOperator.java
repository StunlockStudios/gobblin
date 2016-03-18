package gobblin.stunlock.schemaflattening;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.fork.CopyNotSupportedException;
import gobblin.fork.CopyableGenericRecord;
import gobblin.fork.CopyableSchema;
import gobblin.fork.ForkOperator;

public class FlattenForkOperator implements ForkOperator<CopyableSchema, CopyableGenericRecord>
{
	private static final Logger LOG = LoggerFactory.getLogger(FlattenForkOperator.class);
	
	// Reuse both lists to save the cost of allocating new lists
	private final List<Boolean> schemas = new ArrayList<Boolean>();
	private final List<Boolean> records = new ArrayList<Boolean>();

	public static final String FLATTEN_ARRAY_COUNT = "schemaflattener.arraycount";

	private int arrayCount;

	@Override
	public void init(WorkUnitState workUnitState) throws Exception
	{
		arrayCount = workUnitState.getPropAsInt(FLATTEN_ARRAY_COUNT, -1);
		
		if (arrayCount == -1)
			throw new Exception("Setting " + FLATTEN_ARRAY_COUNT + " has not been configured to a valid number.");
	}

	@Override
	public int getBranches(WorkUnitState workUnitState)
	{
		return this.arrayCount;
	}

	@Override
	public List<Boolean> forkSchema(WorkUnitState workUnitState, CopyableSchema input)
	{
		LOG.info("FORKING SCHEMA TO " + arrayCount + " FORKS");
		fillForkList(workUnitState, schemas);
		return schemas;
	}

	@Override
	public List<Boolean> forkDataRecord(WorkUnitState workUnitState, CopyableGenericRecord input)
	{
		fillForkList(workUnitState, records);
		return records;
	}

	private void fillForkList(WorkUnitState workUnitState, List<Boolean> list)
	{
		list.clear();
		for (int i = 0; i < this.arrayCount; i++)
			list.add(true);
	}

	@Override
	public void close() throws IOException
	{
	}
}
