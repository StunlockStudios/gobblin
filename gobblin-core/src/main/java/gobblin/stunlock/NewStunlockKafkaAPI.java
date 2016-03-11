/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.DatasetFilterUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;

/**
 * Wrapper for the new Kafka API.
 */
public class NewStunlockKafkaAPI implements Closeable
{
	private static final Logger LOG = LoggerFactory.getLogger(NewStunlockKafkaAPI.class);

	KafkaConsumer<byte[], byte[]> consumer;

	public NewStunlockKafkaAPI(State state)
	{
		Preconditions.checkNotNull(state.getProp(ConfigurationKeys.KAFKA_BROKERS), "Need to specify at least one Kafka broker.");
		String kafkaBroker = state.getProp(ConfigurationKeys.KAFKA_BROKERS);

		LOG.info("NewStunlockKafkaAPI Constructor");
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaBroker);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");

		consumer = new KafkaConsumer<byte[], byte[]>(props);
		LOG.info("NewStunlockKafkaAPI Constructed");
	}

	public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist)
	{
		LOG.info("NewStunlockKafkaAPI getFilteredTopics Start");
		Map<String, List<PartitionInfo>> topicMetadataList = fetchTopicMetadataFromBroker(blacklist, whitelist);

		List<KafkaTopic> filteredTopics = Lists.newArrayList();
		for (Entry<String, List<PartitionInfo>> topicMetadata : topicMetadataList.entrySet())
		{
			List<KafkaPartition> partitions = getPartitionsForTopic(topicMetadata);
			LOG.info("Got topic " + topicMetadata.getKey() + " partitions = " + partitions.size());
			filteredTopics.add(new KafkaTopic(topicMetadata.getKey(), partitions));
		}
		LOG.info("NewStunlockKafkaAPI getFilteredTopics End");
		return filteredTopics;
	}

	private List<KafkaPartition> getPartitionsForTopic(Entry<String, List<PartitionInfo>> topicMetadata)
	{
		List<KafkaPartition> partitions = Lists.newArrayList();

		for (PartitionInfo partitionMetadata : topicMetadata.getValue())
		{
			KafkaPartition.Builder builder = new KafkaPartition.Builder().withId(partitionMetadata.partition()).withTopicName(topicMetadata.getKey()).withLeaderId(partitionMetadata.leader().id()).withLeaderHostAndPort(partitionMetadata.leader().host(), partitionMetadata.leader().port());
			partitions.add(builder.build());
		}
		return partitions;
	}

	private Map<String, List<PartitionInfo>> fetchTopicMetadataFromBroker(List<Pattern> blacklist,
			List<Pattern> whitelist)
	{
		Map<String, List<PartitionInfo>> topicMetadataList = fetchTopicMetadataFromBroker();
		if (topicMetadataList == null)
		{
			return null;
		}

		Map<String, List<PartitionInfo>> filteredTopicMetadataList = new HashMap<String, List<PartitionInfo>>();
		for (Entry<String, List<PartitionInfo>> topicMetadata : topicMetadataList.entrySet())
		{
			LOG.info("Got topic " + topicMetadata.getKey());
			if (DatasetFilterUtils.survived(topicMetadata.getKey(), blacklist, whitelist))
			{
				filteredTopicMetadataList.put(topicMetadata.getKey(), topicMetadata.getValue());
			}
		}
		return filteredTopicMetadataList;
	}

	private Map<String, List<PartitionInfo>> fetchTopicMetadataFromBroker()
	{
		return consumer.listTopics();
	}

	public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException
	{
		LOG.info("NewStunlockKafkaAPI getEarliestOffset Start");
		TopicPartition topicAndPartition = new TopicPartition(partition.getTopicName(), partition.getId());
		List<TopicPartition> assignedPartitions = new ArrayList<TopicPartition>();
		assignedPartitions.add(topicAndPartition);
		consumer.assign(assignedPartitions);
		LOG.info("NewStunlockKafkaAPI getEarliestOffset Assigned");
		consumer.seekToBeginning(topicAndPartition);
		LOG.info("NewStunlockKafkaAPI getEarliestOffset seekToBeginning");
		long position = consumer.position(topicAndPartition);
		LOG.info("NewStunlockKafkaAPI getEarliestOffset End");
		return position;
	}

	public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException
	{
		LOG.info("NewStunlockKafkaAPI getLatestOffset Start");
		TopicPartition topicAndPartition = new TopicPartition(partition.getTopicName(), partition.getId());
		List<TopicPartition> assignedPartitions = new ArrayList<TopicPartition>();
		assignedPartitions.add(topicAndPartition);
		consumer.assign(assignedPartitions);
		consumer.seekToEnd(topicAndPartition);
		long position;
		try
		{
			position = consumer.position(topicAndPartition);
		}
		catch (Exception exc)
		{
			LOG.error("Error fetching kafka position for partition " + partition.toString() + ": " + exc);
			throw exc;
		}
		LOG.info("NewStunlockKafkaAPI getLatestOffset End");
		return position;
	}

	public void close() throws IOException
	{
		consumer.close();
	}

	public Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset, long maxOffset)
	{
		LOG.info("NewStunlockKafkaAPI fetchNextMessageBuffer Start");
		LOG.info("fetchNextMessageBuffer on Thread ID: " + Thread.currentThread().getId());
		if (nextOffset > maxOffset)
		{
			return null;
		}
		
		long toFetch = maxOffset - nextOffset;

		TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
		List<TopicPartition> assignedPartitions = new ArrayList<TopicPartition>();
		assignedPartitions.add(topicPartition);
		consumer.assign(assignedPartitions);
		consumer.seek(topicPartition, nextOffset);
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
		while (toFetch != 0)
		{
			ConsumerRecords<byte[], byte[]> recordsFetched = consumer.poll(100);
			boolean finished = false;
			for (ConsumerRecord<byte[], byte[]> record : recordsFetched) {
				records.add(record);
				if (records.size() >= toFetch) {
					finished = true;
					break;
				}
			}
			if (finished)
				break;
		}
		List<MessageAndOffset> messages = new ArrayList<MessageAndOffset>();
		for (ConsumerRecord<byte[], byte[]> record : records)
		{
			messages.add(new MessageAndOffset(new Message(record.value(), record.key()), record.offset()));
		}

		LOG.info("NewStunlockKafkaAPI fetchNextMessageBuffer End");
		return messages.iterator();
	}
}