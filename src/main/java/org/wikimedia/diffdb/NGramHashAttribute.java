package org.wikimedia.diffdb;

import org.apache.lucene.util.*;

public interface NGramHashAttribute extends Attribute {
  int getValue();
  void setValue(int value);
}
