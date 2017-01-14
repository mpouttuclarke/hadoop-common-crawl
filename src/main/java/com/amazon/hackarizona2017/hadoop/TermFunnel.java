package com.amazon.hackarizona2017.hadoop;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * Allows Guava hashing to operate on term ids
 */
class TermFunnel implements Funnel<String> {
    @Override
    public void funnel(String term, PrimitiveSink from) {
        from.putString(term, Charsets.UTF_8);
    }
}
