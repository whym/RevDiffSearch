package org.wikimedia.diffdb;

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
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    init(minGram, maxGram, seed);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param source {@link AttributeSource} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public HashedNGramTokenizer(AttributeSource source, Reader input, int minGram, int maxGram, int seed) {
    super(source, input);
    this.addAttributeImpl(new NGramHashAttributeImpl());
    this.hashAtt   = addAttribute(NGramHashAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    init(minGram, maxGram, seed);
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
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    init(minGram, maxGram, seed);
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
  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    if (!started) {
      started = true;
      gramSize = minGram;
      char[] chars = new char[1024];
      input.read(chars);
      inStr = new String(chars).trim();  // remove any trailing empty strings 
      inLen = inStr.length();
    }

    if (pos+gramSize > inLen) {            // if we hit the end of the string
      pos = 0;                           // reset to beginning of string
      gramSize++;                        // increase n-gram size
      if (gramSize > maxGram)            // we are done
        return false;
      if (pos+gramSize > inLen)
        return false;
    }

    int oldPos = pos;
    pos++;
    byte[] bytes = inStr.substring(oldPos, oldPos+gramSize).getBytes();
    hashAtt.setValue(MurmurHash.hash32(bytes, bytes.length, this.seed));
    offsetAtt.setOffset(correctOffset(oldPos), correctOffset(oldPos+gramSize));
    return true;
  }
  
  @Override
  public final void end() {
    // set final offset
    final int finalOffset = inLen;
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }    
  
  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    reset();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    started = false;
    pos = 0;
  }
}
