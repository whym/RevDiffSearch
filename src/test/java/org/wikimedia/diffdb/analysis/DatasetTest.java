package org.wikimedia.diffdb.analysis;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;

public class DatasetTest {
	
	private Dataset dataset;
	
	@Before
	public void setUp() throws Exception {
		dataset = new Dataset("foo");
	}

	@Test
	public void testConvertTimestamptoDate() {
		String timestamp = "2009-10-19T10:47:34Z";
		Date date = Dataset.convertTimestamptoDate(timestamp);
		assertEquals(Date.class, date.getClass());
	}

	@Test
	public void testAddDate() {
		String timestamp = "2009-10-19T10:47:34Z";
		dataset.addDate(timestamp);
		assertEquals("Result", true, dataset.containsYear(2009));
		assertEquals("Result", true, dataset.containsMonth(2009, 10));
	}

	@Test
	public void testWriteDataset() {
		fail("Not yet implemented");
	}

}
