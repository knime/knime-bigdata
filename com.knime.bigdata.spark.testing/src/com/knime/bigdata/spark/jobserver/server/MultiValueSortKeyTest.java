package com.knime.bigdata.spark.jobserver.server;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.knime.bigdata.spark.UnitSpec;

/**
 *
 * @author dwk
 *
 */
public class MultiValueSortKeyTest extends UnitSpec {

    /**
     *
     */
    @Test
    public void comparingKeyWithItselfShouldBe0() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        assertEquals("compare with itself", 0, key1.compareTo(key1));
    }

    @Test
    public void comparingKeyWithSameOtherShouldBe0() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        
        assertEquals("compare with same other", 0, key1.compareTo(key2));
    }

    @Test
    public void comparingKeysWithNullValues() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{null, 0.1}, new Boolean[] {true, true});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        
        final MultiValueSortKey[] vals = new MultiValueSortKey[] {key1, key2};
        Arrays.sort(vals);
        assertEquals("without null first", key2, vals[0]);
        assertEquals("with null second", key1, vals[1]);
    }

    @Test
    public void comparingKeyWithLargerOtherShouldBe1WhenDescending() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {false, true});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"b", 0.1}, new Boolean[] {false, true});
        
        assertEquals("compare with larger other", 1, key1.compareTo(key2));
        final MultiValueSortKey[] vals = new MultiValueSortKey[] {key2, key1};
        Arrays.sort(vals);
        assertEquals("larger first", key2, vals[0]);
        assertEquals("smaller second", key1, vals[1]);
    }

    @Test
    public void comparingKeyWithLargerOtherShouldBeNeg1WhenAsscending() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"b", 0.1}, new Boolean[] {true, true});
        
        assertEquals("compare with larger other", -1, key1.compareTo(key2));
        final MultiValueSortKey[] vals = new MultiValueSortKey[] {key2, key1};
        Arrays.sort(vals);
        assertEquals("smallest first", key1, vals[0]);
        assertEquals("larger second", key2, vals[1]);
    }
    
    @Test
    public void comparingKeyWithLargerOtherShouldBeNeg1WhenAsscendingMultiLevel() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.01}, new Boolean[] {true, true});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, true});
        
        assertEquals("compare with larger other", -1, key1.compareTo(key2));
        final MultiValueSortKey[] vals = new MultiValueSortKey[] {key2, key1};
        Arrays.sort(vals);
        assertEquals("smallest first", key1, vals[0]);
        assertEquals("larger second", key2, vals[1]);
    }
    
    @Test
    public void comparingKeyWithLargerOtherShouldBeNeg1WhenDescendingMultiLevel() {
        final MultiValueSortKey key1 = new MultiValueSortKey(new Object[]{"a", 0.01}, new Boolean[] {true, false});
        final MultiValueSortKey key2 = new MultiValueSortKey(new Object[]{"a", 0.1}, new Boolean[] {true, false});
        
        assertEquals("compare with larger other", 1, key1.compareTo(key2));
        final MultiValueSortKey[] vals = new MultiValueSortKey[] {key2, key1};
        Arrays.sort(vals);
        assertEquals("smallest first", key2, vals[0]);
        assertEquals("larger second", key1, vals[1]);
    }

}
