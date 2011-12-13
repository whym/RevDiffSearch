package org.wikimedia.diffdb;

import org.apache.lucene.util.*;

public class NGramHashAttributeImpl extends AttributeImpl implements NGramHashAttribute {
  private int val;
  public int getValue() {
    return this.val;
  }  
  public void setValue(int value) {
    this.val = value;
  }
  public boolean equals(Object o) {
    if ( o instanceof NGramHashAttribute ) {
      NGramHashAttribute a = (NGramHashAttribute)o;
      return a.getValue() == this.getValue();
    }
    return false;
  }
  public void clear() {
    this.val = 0;
  }
  public void copyTo(AttributeImpl target) {
    if ( target instanceof NGramHashAttribute ) {
      NGramHashAttribute a = (NGramHashAttribute)target;
      a.setValue(this.getValue());
    } else {
      throw new IllegalArgumentException("incompatible target: " + target);
    }
  }
  public String toString() {
    return "" + this.val;
  }
}