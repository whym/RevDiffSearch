package org.wikimedia.diffdb;
import org.junit.*;
import java.io.*;
import java.util.*;
import java.net.*;
import static org.junit.Assert.*;

public class TestDiffCollector {
  @Test public void testGetQueryFields() {
    Map<String,String> expect = new TreeMap<String,String>();
    expect.put("added", "help");
    assertEquals(expect, DiffCollector.getQueryFields("added:help"));
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

