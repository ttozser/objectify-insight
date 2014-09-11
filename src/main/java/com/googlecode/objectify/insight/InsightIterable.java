package com.googlecode.objectify.insight;

import com.google.appengine.api.datastore.Entity;
import lombok.RequiredArgsConstructor;
import java.util.Iterator;

/**
 */
@RequiredArgsConstructor
public class InsightIterable implements Iterable<Entity> {

	private final Iterable<Entity> raw;

	protected final Recorder recorder;

	protected final String query;

	private boolean collected;

	@Override
	public Iterator<Entity> iterator() {
		if (collected) {
			return raw.iterator();
		} else {
			collected = true;
			return InsightIterator.create(raw.iterator(), recorder, query);
		}
	}
}
