package org.wikimedia.diffdb;

public class Pair<X,Y> {
  protected X first;
  protected Y second;
  public Pair(X x, Y y) {
    this.first = x;
    this.second = y;
  }
  public Pair(Pair<X,Y> p) {
    this(p.first, p.second);
  }

  public X getFirst()  { return this.first; }
  public Y getSecond() { return this.second; }
  public void setFirst(X c)  { this.first  = c; }
  public void setSecond(Y c) { this.second = c; }
  @Override
    public boolean equals(Object o) {
    if ( this == o ) return true;
    if ( o instanceof Pair ) {
      Pair p = (Pair)o;
      return this.first.equals(p.first) && this.second.equals(p.second);
    } else {
      return false;
    }
  }
  @Override public int hashCode() {
    return this.first.hashCode() * 107 + this.second.hashCode();
  }

  @Override
    public String toString() {
    return "(" + this.first + "," + this.second + ")";
  }
  public static <Z,W> Pair<Z,W> newInstance(Z z, W w) {
    return new Pair<Z,W>(z,w);
  }
  public static <Z,W> Pair<Z,W> newInstance(Pair<Z,W> p) {
    return new Pair<Z,W>(p);
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */
