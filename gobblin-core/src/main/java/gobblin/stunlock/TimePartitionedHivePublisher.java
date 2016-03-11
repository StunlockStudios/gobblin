package gobblin.stunlock;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import gobblin.configuration.State;
import gobblin.publisher.BaseDataPublisher;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;
import gobblin.util.FileListUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import com.google.common.base.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import java.util.Properties;

/**
 * For time partition jobs, writer output directory is
 * $GOBBLIN_WORK_DIR/task-output/{extractId}/{tableName}/{partitionPath}, where
 * partition path is the time bucket, e.g., 2015/04/08/15.
 *
 * Publisher output directory is
 * $GOBBLIN_WORK_DIR/job-output/{tableName}/{partitionPath}
 *
 * @author ziliu
 */
public class TimePartitionedHivePublisher extends BaseDataPublisher
{
	private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedHivePublisher.class);

	// @formatter:off
	private static final String CREATE_TABLE_QUERY = 
			"CREATE EXTERNAL TABLE IF NOT EXISTS %1$s " + 
			"PARTITIONED BY (date_and_hour string) " + 
			"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " + 
			"STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " + 
			"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " + 
			"LOCATION '%2$s' " + 
			"TBLPROPERTIES ('avro.schema.url'='%3$s');";

	private static final String ALTER_TABLE_QUERY = 
			"ALTER TABLE %1$s " + 
			"ADD IF NOT EXISTS " + 
			"PARTITION (date_and_hour='%2$s');";

	private static final String REFRESH_TABLE_QUERY = 
			"MSCK REPAIR TABLE %1$s;";
	
	private static final String PUBLISHER_SCHEMANAME = "writer.partitioner.schemaname";
	// @formatter:on

	public TimePartitionedHivePublisher(State state) throws IOException
	{
		super(state);
	}

	@Override
	protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
			Set<Path> writerOutputPathsMoved) throws IOException
	{
		//RegisterInHive(state, branchId);
		super.publishData(state, branchId, publishSingleTaskData, writerOutputPathsMoved);
	}

	private void RegisterInHive(WorkUnitState state, int branchId)
	{
		// @formatter:off
		// Hoping that this contains what HiveJdbcConnector needs. Verify it.
		// @formatter:on
		Properties properties = state.getProperties();

		HiveJdbcConnector conn;
		Closer closer = Closer.create();
		try
		{
			// @formatter:off
			// [DONE?] Need to partition by SchemaID as well as DateHour?
			// Can we pass on data from converters or forks to publishers? Try, but probably not, since we're dealing with Forked executions etc.
			// Table needs a SchemaID. Parse from Data Path?
			// Job's can't take data from different schemas, unless it's partitioned by it as well.
			// @formatter:on

			// Register in HIVE?
			int schemaID = 0; // GET FROM PATH?
			String hdfsBaseUrl = ""; // GET FROM PROPERTIES
			String partitionDateAndHour = null; // GET FROM PATH?

			String outputSchemaName = GetSchemaName(properties, branchId);
			String schemaRegistryBaseUrl = GetSchemaRegistryBaseUrl(properties);
			String schemaURL = schemaRegistryBaseUrl + "schemas/ids/" + schemaID;
			String tableName = outputSchemaName + "_" + schemaID;

			// I have a vague memory that the tableName needed to be the same as
			// the root output folder.
			String hdfsTableRootLocation = hdfsBaseUrl + Path.SEPARATOR + tableName;

			String createTableStmt = String.format(CREATE_TABLE_QUERY, tableName, hdfsTableRootLocation, schemaURL);
			String alterTableStmt = String.format(ALTER_TABLE_QUERY, tableName, partitionDateAndHour);
			String refreshTableStmt = String.format(REFRESH_TABLE_QUERY, tableName);

			conn = closer.register(HiveJdbcConnector.newConnectorWithProps(properties));
			conn.executeStatements(createTableStmt, alterTableStmt, refreshTableStmt);
		}
		catch (SQLException e)
		{
			LOG.error(e.toString());
		}
		finally
		{
			try
			{
				closer.close();
			}
			catch (IOException e)
			{
				LOG.error(e.toString());
			}
		}
	}

	private String GetSchemaRegistryBaseUrl(Properties properties)
	{
		return properties.getProperty(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);
	}

	private String GetSchemaName(Properties properties, int branchId)
	{
		String propertyName = PUBLISHER_SCHEMANAME + "." + branchId;
		if (properties.contains(propertyName) == false)
		{
			LOG.error("EXPECTED ARRAY NAME PROPERTY '" + propertyName + "' NOT SET!");
		}

		return properties.getProperty(propertyName);
	}

	/**
	 * This method needs to be overridden for TimePartitionedDataPublisher,
	 * since the output folder structure contains timestamp, we have to move the
	 * files recursively.
	 *
	 * For example, move {writerOutput}/2015/04/08/15/output.avro to
	 * {publisherOutput}/2015/04/08/15/output.avro
	 */
	@Override
	protected void addWriterOutputToExistingDir(Path writerOutput, Path publisherOutput, WorkUnitState workUnitState,
			int branchId, ParallelRunner parallelRunner) throws IOException
	{
		for (FileStatus status : FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(branchId), writerOutput))
		{
			String filePathStr = status.getPath().toString();
			String pathSuffix = filePathStr.substring(filePathStr.indexOf(writerOutput.toString()) + writerOutput.toString().length() + 1);
			Path outputPath = new Path(publisherOutput, pathSuffix);

			WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId), outputPath.getParent(), this.permissions.get(branchId));

			LOG.info(String.format("Moving %s to %s", status.getPath(), outputPath));
			parallelRunner.movePath(status.getPath(), this.publisherFileSystemByBranches.get(branchId), outputPath, Optional.<String> absent());
		}
	}
}
