package com.googlecode.objectify.insight.test;

import com.googlecode.objectify.insight.Bucket;
import com.googlecode.objectify.insight.Flusher;
import com.googlecode.objectify.insight.InsightCollector;
import com.googlecode.objectify.insight.test.util.TestBase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 */
public class InsightCollectorTest extends TestBase {

	private Flusher flusher;
	private InsightCollector collector;

	@BeforeMethod
	public void setUpFixture() throws Exception {
		flusher = mock(Flusher.class);
		collector = new InsightCollector(flusher);
	}

	@Test
	public void tooManyDifferentBucketsCausesFlush() throws Exception {
		collector.setSizeThreshold(2);

		Set<Bucket> buckets = new LinkedHashSet<>();
		buckets.add(Bucket.forGet("ns", "kindA", 1));
		buckets.add(Bucket.forGet("ns", "kindB", 1));

		Iterator<Bucket> it = buckets.iterator();

		collector.collect(it.next());

		verify(flusher, never()).flush(anyCollectionOf(Bucket.class));

		collector.collect(it.next());

		verify(flusher).flush(buckets(buckets));
	}

	@Test
	public void ageCausesFlush() throws Exception {
		collector.setAgeThresholdMillis(100);

		Bucket bucket = Bucket.forGet("ns", "kindA", 1);

		collector.collect(bucket);
		collector.collect(bucket);

		verify(flusher, never()).flush(anyCollectionOf(Bucket.class));

		Thread.sleep(101);
		collector.collect(bucket);

		verify(flusher).flush(buckets(Bucket.forGet("ns", "kindA", 3)));
	}

	@Test
	public void sameBucketOverAndOverDoesNotCauseFlush() throws Exception {
		collector.setSizeThreshold(2);

		for (int i=0; i<10; i++)
			collector.collect(Bucket.forGet("ns", "kind", 1));

		verify(flusher, never()).flush(anyCollectionOf(Bucket.class));
	}

	@Test
	public void sameBucketGetsAggregated() throws Exception {
		collector.setSizeThreshold(2);

		collector.collect(Bucket.forGet("ns", "kindA", 1));
		collector.collect(Bucket.forGet("ns", "kindA", 2));
		collector.collect(Bucket.forGet("ns", "kindA", 3));

		verify(flusher, never()).flush(anyCollectionOf(Bucket.class));

		collector.collect(Bucket.forGet("ns", "kindB", 1));

		Set<Bucket> expected = new LinkedHashSet<>();
		expected.add(Bucket.forGet("ns", "kindA", 6));
		expected.add(Bucket.forGet("ns", "kindB", 1));

		verify(flusher).flush(buckets(expected));
	}
}