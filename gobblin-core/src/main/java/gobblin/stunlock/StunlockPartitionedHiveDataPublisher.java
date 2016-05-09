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
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.BaseDataPublisher;
import gobblin.publisher.TimePartitionedDataPublisher;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;

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
public class StunlockPartitionedHiveDataPublisher extends BaseDataPublisher
{
	private static final Logger LOG = LoggerFactory.getLogger(StunlockPartitionedHiveDataPublisher.class);

	// @formatter:off
	private static final String CREATE_TABLE_QUERY = 
			"CREATE EXTERNAL TABLE IF NOT EXISTS %1$s " + 
			"PARTITIONED BY (date_and_hour string) " + 
			"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " + 
			"STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " + 
			"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " + 
			"LOCATION '%2$s' " + 
			"TBLPROPERTIES ('avro.schema.url'='%3$s')";

	private static final String ALTER_TABLE_QUERY = 
			"ALTER TABLE %1$s " + 
			"ADD IF NOT EXISTS " + 
			"PARTITION (date_and_hour='%2$s') LOCATION '%3$s'";

	private static final String REFRESH_TABLE_QUERY = 
			"MSCK REPAIR TABLE %1$s";
	
	private static final String PUBLISHER_SCHEMANAME = "stun.writer.partitioner.schemaname";
	private static final String HIVE_URL = "stun.publisher.hive.url";
	private static final String HIVE_USER = "stun.publisher.hive.user";
	private static final String HIVE_PASSWORD = "stun.publisher.hive.password";
	// @formatter:on

	public StunlockPartitionedHiveDataPublisher(State state) throws IOException
	{
		super(state);
	}

	// THIS IS FROM TimePartitionedDataPublisher
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

			movePath(parallelRunner, this.getState(), status.getPath(), outputPath, branchId);
			RegisterInHive(workUnitState, outputPath.toString(), branchId);
		}
	}

	private void RegisterInHive(WorkUnitState state, String pathStr, int branchId)
	{
		LOG.info("RegisterInHive: " + pathStr);

		// HiveJdbcConnector conn;
		Closer closer = Closer.create();
		try
		{
			String outputSchemaName = null;
			if (PropertyExists(state, branchId, PUBLISHER_SCHEMANAME))
				outputSchemaName = GetProperty(state, branchId, PUBLISHER_SCHEMANAME);
			else
				outputSchemaName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

			Path filePath = new Path(pathStr);
			Path dateHourFolder = filePath.getParent();
			Path dateDayFolder = dateHourFolder.getParent();
			Path dateMonthFolder = dateDayFolder.getParent();
			Path dateYearFolder = dateMonthFolder.getParent();
			String fullDatePartitionFolderName = dateYearFolder.getName() + "/" + dateMonthFolder.getName() + "/" + dateDayFolder.getName() + "/" + dateHourFolder.getName();

			Path schemaNameAndIdFolder = dateYearFolder.getParent();
			Path hdfsDataBaseFolder = schemaNameAndIdFolder.getParent();

			String schemaIdFolderPrefix = outputSchemaName + "_";
			String schemaNameAndIdFolderStr = schemaNameAndIdFolder.getName();
			if (schemaNameAndIdFolderStr.startsWith(schemaIdFolderPrefix) == false)
			{
				LOG.error("Path does not match expected format. No folder for schema id partitions in " + pathStr);
				return;
			}
			String schemaIdStr = schemaNameAndIdFolderStr.substring(schemaIdFolderPrefix.length());

			// @formatter:off
			// [DONE?] Need to partition by SchemaID as well as DateHour?
			// Can we pass on data from converters or forks to publishers? Try, but probably not, since we're dealing with Forked executions etc.
			// Table needs a SchemaID. Parse from Data Path?
			// Job's can't take data from different schemas, unless it's partitioned by it as well.
			// @formatter:on

			// Register in HIVE?
			String hdfsBaseUrl = hdfsDataBaseFolder.toString();
			int schemaID = Integer.parseInt(schemaIdStr);

			String schemaRegistryBaseUrl = GetSchemaRegistryBaseUrl(state);
			String schemaURL = schemaRegistryBaseUrl + "/schemas/ids/plain/" + schemaID;
			String tableName = schemaNameAndIdFolderStr; // I have a vague
															// memory that the
															// tableName needed
															// to be the same as
															// the root output
															// folder.

			String hiveUrl = GetProperty(state, branchId, HIVE_URL);
			String hiveUser = GetProperty(state, branchId, HIVE_USER);
			String hivePassword = GetProperty(state, branchId, HIVE_PASSWORD);

			String hdfsTableRootLocation = hdfsBaseUrl + Path.SEPARATOR + tableName;

			String createTableStmt = String.format(CREATE_TABLE_QUERY, tableName, hdfsTableRootLocation, schemaURL);
			String alterTableStmt = String.format(ALTER_TABLE_QUERY, tableName, fullDatePartitionFolderName, fullDatePartitionFolderName);
			String refreshTableStmt = String.format(REFRESH_TABLE_QUERY, tableName);

			LOG.info("Time to register, Q1: " + createTableStmt);
			LOG.info("Time to register, Q2: " + alterTableStmt);
			LOG.info("Time to register, Q3: " + refreshTableStmt);
			StunHiveClient.ExecuteStatements(hiveUrl, hiveUser, hivePassword, createTableStmt, alterTableStmt, refreshTableStmt);

			// Old
			// conn =
			// closer.register(HiveJdbcConnector.newConnectorWithProps(properties));
			// conn.executeStatements(createTableStmt, alterTableStmt,
			// refreshTableStmt);
			LOG.info("Register done");
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

	private String GetSchemaRegistryBaseUrl(WorkUnitState state)
	{
		return state.getProp(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);
	}

	private Boolean PropertyExists(WorkUnitState state, int branchId, String propertyName)
	{
		if(state.contains(ForkOperatorUtils.getPropertyNameForBranch(propertyName, branchId)))
			return true;
		
		else if(state.contains(propertyName))
			return true;
		
		return false;
	}

	private String GetProperty(WorkUnitState state, int branchId, String propertyName)
	{
		String prop = ForkOperatorUtils.getPropertyNameForBranch(propertyName, branchId);

		if (state.contains(prop))
			return state.getProp(prop);
		
		if (state.contains(propertyName))
			return state.getProp(propertyName);

		LOG.error("EXPECTED PROPERTY '" + prop + "' NOT SET, NEITHER IN BRANCH " + branchId + " NOR AS UNBRANCHED SETTING!");
		return null;
	}
}
