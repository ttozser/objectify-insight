package com.googlecode.objectify.insight.puller.test;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.googlecode.objectify.insight.Bucket;
import com.googlecode.objectify.insight.BucketFactory;
import com.googlecode.objectify.insight.BucketList;
import com.googlecode.objectify.insight.Clock;
import com.googlecode.objectify.insight.puller.BigUploader;
import com.googlecode.objectify.insight.puller.Puller;
import com.googlecode.objectify.insight.test.util.TestBase;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 */
public class PullerTest extends TestBase {

	@Mock private Queue queue;
	@Mock private BigUploader bigUploader;

	private Puller puller;

	@BeforeMethod
	public void setUpFixture() throws Exception {
		puller = new Puller(queue, bigUploader);
	}

	@Test
	public void uploadsAggregatedLeasedTasksByKind() throws Exception {
		BucketFactory bucketFactory = new BucketFactory();
		
		Bucket bucket1 = bucketFactory.forGet("here", "ns", "kindA", 11);
		Bucket bucket2 = bucketFactory.forGet("here", "ns", "kindB", 22);
		Bucket bucket3 = bucketFactory.forGet("here", "ns", "kindA", 33);
		Bucket bucket4 = bucketFactory.forGet("here", "ns", "kindB", 44);
		
		BucketList bucketList1 = new BucketList(Arrays.asList(bucket1, bucket2));
		BucketList bucketList2 = new BucketList(Arrays.asList(bucket3, bucket4));

		TaskHandle taskHandle1 = makeTaskHandle(bucketList1);
		TaskHandle taskHandle2 = makeTaskHandle(bucketList2);

		when(queue.leaseTasks(Puller.DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS, Puller.DEFAULT_BATCH_SIZE))
				.thenReturn(Arrays.asList(taskHandle1, taskHandle2));

		puller.execute();

		verify(queue).deleteTaskAsync(Arrays.asList(taskHandle1, taskHandle2));

		verify(bigUploader).upload(buckets(Arrays.asList(
				bucketFactory.forGet("here", "ns", "kindA", 44),
				bucketFactory.forGet("here", "ns", "kindB", 66)
		)));
	}

	@Test
	public void uploadsAggregatedLeasedTasksByService() throws Exception {
		BucketFactory bucketFactory1 = new BucketFactory(new Clock(), "serviceA", "version");
		Bucket bucket1 = bucketFactory1.forGet("here", "ns", "kindA", 11);
		Bucket bucket2 = bucketFactory1.forGet("here", "ns", "kindA", 22);

		BucketFactory bucketFactory2 = new BucketFactory(new Clock(), "serviceB", "version");
		Bucket bucket3 = bucketFactory2.forGet("here", "ns", "kindA", 33);
		Bucket bucket4 = bucketFactory2.forGet("here", "ns", "kindA", 44);
		
		BucketList bucketList1 = new BucketList(Arrays.asList(bucket1, bucket2));
		BucketList bucketList2 = new BucketList(Arrays.asList(bucket3, bucket4));

		TaskHandle taskHandle1 = makeTaskHandle(bucketList1);
		TaskHandle taskHandle2 = makeTaskHandle(bucketList2);

		when(queue.leaseTasks(Puller.DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS, Puller.DEFAULT_BATCH_SIZE))
				.thenReturn(Arrays.asList(taskHandle1, taskHandle2));

		puller.execute();

		verify(queue).deleteTaskAsync(Arrays.asList(taskHandle1, taskHandle2));

		verify(bigUploader).upload(buckets(Arrays.asList(
				bucketFactory1.forGet("here", "ns", "kindA", 33),
				bucketFactory2.forGet("here", "ns", "kindA", 77)
		)));
	}

	@Test
	public void uploadsAggregatedLeasedTasksByVersion() throws Exception {
		BucketFactory bucketFactory1 = new BucketFactory(new Clock(), "service", "versionA");
		Bucket bucket1 = bucketFactory1.forGet("here", "ns", "kindA", 11);
		Bucket bucket2 = bucketFactory1.forGet("here", "ns", "kindA", 22);
		
		BucketFactory bucketFactory2 = new BucketFactory(new Clock(), "service", "versionB");
		Bucket bucket3 = bucketFactory2.forGet("here", "ns", "kindA", 33);
		Bucket bucket4 = bucketFactory2.forGet("here", "ns", "kindA", 44);
		
		BucketList bucketList1 = new BucketList(Arrays.asList(bucket1, bucket2));
		BucketList bucketList2 = new BucketList(Arrays.asList(bucket3, bucket4));
		
		TaskHandle taskHandle1 = makeTaskHandle(bucketList1);
		TaskHandle taskHandle2 = makeTaskHandle(bucketList2);
		
		when(queue.leaseTasks(Puller.DEFAULT_LEASE_DURATION_SECONDS, TimeUnit.SECONDS, Puller.DEFAULT_BATCH_SIZE))
		.thenReturn(Arrays.asList(taskHandle1, taskHandle2));
		
		puller.execute();
		
		verify(queue).deleteTaskAsync(Arrays.asList(taskHandle1, taskHandle2));
		
		verify(bigUploader).upload(buckets(Arrays.asList(
				bucketFactory1.forGet("here", "ns", "kindA", 33),
				bucketFactory2.forGet("here", "ns", "kindA", 77)
				)));
	}
}
