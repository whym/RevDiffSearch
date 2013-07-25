package org.wikimedia.revdiffsearch;

/**
 * Copyright 2011 Apache Software Foundation, Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource;
import ie.ucd.murmur.MurmurHash;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.Reader;

/**
 * Tokenizes the input into n-grams of the given size(s).
 */
public final class HashedNGramTokenizer extends Tokenizer {
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;
  public static final int DEFAULT_SEED = 97;

  private int minGram, maxGram;
  private int seed;
  private int gramSize;
  private int pos = 0;
  private int inLen;
  private String inStr;
  private boolean started = false;
  
  private final NGramHashAttribute hashAtt;
  private final CharTermAttribute termAtt;
  private final OffsetAttribute offsetAtt;

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public HashedNGramTokenizer(Reader input, int minGram, int maxGram, int seed) {
    super(input);
    this.addAttributeImpl(new NGramHashAttributeImpl());
    this.hashAtt   = addAttribute(NGramHashAttribute.class);
    this.termAtt   = addAttribute(CharTermAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.init(minGram, maxGram, seed);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public HashedNGramTokenizer(AttributeFactory factory, Reader input, int minGram, int maxGram, int seed) {
    super(factory, input);
    this.addAttributeImpl(new NGramHashAttributeImpl());
    this.hashAtt   = addAttribute(NGramHashAttribute.class);
    this.termAtt   = addAttribute(CharTermAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.init(minGram, maxGram, seed);
  }

  /**
   * Creates NGramTokenizer with default min and max n-grams.
   * @param input {@link Reader} holding the input to be tokenized
   */
  public HashedNGramTokenizer(Reader input) {
    this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE, DEFAULT_SEED);
  }
  
  private void init(int minGram, int maxGram, int seed) {
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    this.seed = seed;
  }

  /** Returns the next token in the stream, or null at EOS. */
  public final boolean incrementToken() throws IOException {
    this.clearAttributes();
    if (!this.started) {
      this.started = true;
      this.gramSize = this.minGram;
      char[] chars = new char[1024];
      this.input.read(chars);
      this.inStr = new String(chars).trim();  // remove any trailing empty strings 
      this.inLen = this.inStr.length();
    }

    if (this.pos+this.gramSize > this.inLen) {            // if we hit the end of the string
      return false;
    }

    int oldPos = pos;
    int hash = this.hashString(this.inStr.substring(oldPos, oldPos+this.gramSize));
    //this.hashAtt.setValue(hash);  // TODO: NGramHashAttribute didn't work as index token in search; disabled until we find a better way to embed the hash value
    char[] str = this.encodeIntegerAsString(hash).toCharArray();
    this.termAtt.copyBuffer(str, 0, str.length); // for now we embed integers as Base64 encoded strings
    this.offsetAtt.setOffset(this.correctOffset(oldPos), this.correctOffset(oldPos+gramSize));
    this.gramSize++;
    if (this.gramSize > this.maxGram) {
      this.pos++;
      this.gramSize = this.minGram;
    }
    return true;
  }

  public int hashString(String str) {
    byte[] bytes = str.getBytes();
    return MurmurHash.hash32(bytes, bytes.length, this.seed);
  }

  public static String encodeIntegerAsString(int x) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte)(x & 0xFF);
    bytes[1] = (byte)((x >>> 8) & 0xFF);
    bytes[2] = (byte)((x >>> 16) & 0xFF);
    bytes[3] = (byte)((x >>> 24) & 0xFF);
    return Base64.encodeBase64String(bytes).substring(0, 6); // an integer will be 8 characters like ABCDEF==
  }
  
  @Override
  public final void end() {
    // set final offset
    final int finalOffset = this.inLen;
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    this.started = false;
    this.pos = 0;
  }
}
