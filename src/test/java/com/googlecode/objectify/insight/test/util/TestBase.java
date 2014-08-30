/*
 */

package com.googlecode.objectify.insight.test.util;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.googlecode.objectify.insight.Bucket;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.util.Collection;
import java.util.Collections;
import static org.mockito.Matchers.argThat;

/**
 * All tests should extend this class to set up the GAE environment.
 * @see <a href="http://code.google.com/appengine/docs/java/howto/unittesting.html">Unit Testing in Appengine</a>
 *
 * @author Jeff Schnitzer <jeff@infohazard.org>
 */
public class TestBase
{
	/** */
	private final LocalServiceTestHelper helper =
			new LocalServiceTestHelper(
					// Our tests assume strong consistency
					new LocalDatastoreServiceTestConfig().setApplyAllHighRepJobPolicy(),
					new LocalTaskQueueTestConfig());
	/** */
	@BeforeMethod
	public void setUp() {
		this.helper.setUp();
	}

	/** */
	@AfterMethod
	public void tearDown() {
		this.helper.tearDown();
	}

	/** */
	protected void runInNamespace(String namespace, Runnable runnable) {
		String oldNamespace = NamespaceManager.get();
		try {
			NamespaceManager.set(namespace);

			runnable.run();
		} finally {
			NamespaceManager.set(oldNamespace);
		}
	}

	/** Little bit of boilerplate that makes the tests read better */
	protected Collection<Bucket> buckets(Collection<Bucket> matching) {
		return argThat(new BucketsMatcher(matching));
	}

	/** Little bit of boilerplate that makes the tests read better */
	protected Collection<Bucket> buckets(Bucket singletonSetContent) {
		return buckets(Collections.singleton(singletonSetContent));
	}

}