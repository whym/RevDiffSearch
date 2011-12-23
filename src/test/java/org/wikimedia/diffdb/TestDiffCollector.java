package org.wikimedia.diffdb;
import org.junit.*;
import java.io.*;
import java.util.*;
import java.net.*;
import static org.junit.Assert.*;

public class TestDiffCollector {
  @Test public void testGetQueryFieldsSingle() {
    Map<String,Set<String>> expect = new HashMap<String,Set<String>>();
    expect.put("added", Collections.singleton("help"));
    assertEquals(expect, DiffCollector.getQueryFields("added:help"));
  }
  @Test public void testGetQueryFieldsWithTwoValue() {
    Map<String,Set<String>> expect = new HashMap<String,Set<String>>();
    expect.put("added", new HashSet<String>(Arrays.asList(new String[]{"help", "me"})));
    assertEquals(expect, DiffCollector.getQueryFields("added:help added:me"));
  }
  @Test public void testGetQueryFieldsWithIncludedValues() {
    Map<String,Set<String>> expect = new HashMap<String,Set<String>>();
    expect.put("added", Collections.singleton("help"));
    assertEquals(expect, DiffCollector.getQueryFields("added:help added:hel"));
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */
