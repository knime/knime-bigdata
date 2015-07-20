package com.knime.bigdata.spark.testing.jobserver.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.MyRecord;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.NominalValueMappingFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class NominalValueMappingTest {

    @Test
    public void emptyColumnMappingShouldHaveSize0() throws Exception {
        final Map<Integer, Map<String, Integer>> mapping = new HashMap<>();
        NominalValueMapping testObj = NominalValueMappingFactory.createColumnMapping(mapping);
        assertEquals("empty mapping should have size 0", 0, testObj.size());
        assertFalse("empty mapping should have empty iterator", testObj.iterator().hasNext());
    }

    @Test
    public void emptyGlobalMappingShouldHaveSize0() throws Exception {
        final Map<String, Integer> mapping = new HashMap<>();
        NominalValueMapping testObj = NominalValueMappingFactory.createGlobalMapping(mapping);
        assertEquals("empty mapping should have size 0", 0, testObj.size());
        assertFalse("empty mapping should have empty iterator", testObj.iterator().hasNext());
    }

    @Test
    public void iteratorForColumnMappingShouldIterateOverAllRecords() throws Exception {
        final Map<Integer, Map<String, Integer>> mapping = new HashMap<Integer, Map<String, Integer>>();
        {
            final Map<String, Integer> colMapping = new HashMap<>();
            colMapping.put("val1", 0);
            colMapping.put("val2", 1);
            colMapping.put("val3", 2);

            mapping.put(1, colMapping);
        }
        {
            final Map<String, Integer> colMapping = new HashMap<>();
            colMapping.put("XXXval1", 0);
            colMapping.put("XXXval2", 1);
            colMapping.put("YYYval3", 2);
            colMapping.put("YYYval88", 3);
            mapping.put(7, colMapping);
        }

        NominalValueMapping testObj = NominalValueMappingFactory.createColumnMapping(mapping);
        assertEquals("mapping should have proper size", 7, testObj.size());
        Iterator<MyRecord> iterator = testObj.iterator();
        assertTrue("non-empty mapping should have non-empty iterator", iterator.hasNext());
        for (int i = 0; i < 7; i++) {
            MyRecord r = iterator.next();
            if (r.m_nominalColumnIndex == 1) {
                assertEquals("column should be", 1, r.m_nominalColumnIndex);
                if (r.m_nominalValue.equals("val1")) {
                    assertEquals("number value for 'val1' should be", 0, r.m_numberValue);
                } else if (r.m_nominalValue.equals("val2")) {
                    assertEquals("number value for 'val2' should be", 1, r.m_numberValue);
                } else if (r.m_nominalValue.equals("val3")) {
                    assertEquals("number value for 'val3' should be", 2, r.m_numberValue);
                } else {
                    fail("unexpected nominal value: " + r.m_nominalValue);
                }
            } else if (r.m_nominalColumnIndex == 7) {
                assertEquals("column should be", 7, r.m_nominalColumnIndex);
                if (r.m_nominalValue.equals("XXXval1")) {
                    assertEquals("number value for 'XXXval1' should be", 0, r.m_numberValue);
                } else if (r.m_nominalValue.equals("XXXval2")) {
                    assertEquals("number value for 'XXXval2' should be", 1, r.m_numberValue);
                } else if (r.m_nominalValue.equals("YYYval3")) {
                    assertEquals("number value for 'YYYval3' should be", 2, r.m_numberValue);
                } else if (r.m_nominalValue.equals("YYYval88")) {
                    assertEquals("number value for 'YYYval88' should be", 3, r.m_numberValue);
                } else {
                    fail("unexpected nominal value: " + r.m_nominalValue);
                }
            } else {
                fail("unexpected column index: " + r.m_nominalColumnIndex);
            }
        }
    }

    @Test
    public void iteratorForGlobalMappingShouldIterateOverAllRecords() throws Exception {
        final Map<String, Integer> mapping = new HashMap<>();

        mapping.put("val1", 0);
        mapping.put("val2", 1);
        mapping.put("val3", 2);

        mapping.put("XXXval1", 3);
        mapping.put("XXXval2", 4);
        mapping.put("YYYval3", 5);
        mapping.put("YYYval88", 6);

        NominalValueMapping testObj = NominalValueMappingFactory.createGlobalMapping(mapping);
        assertEquals("mapping should have proper size", 7, testObj.size());
        Iterator<MyRecord> iterator = testObj.iterator();
        assertTrue("non-empty mapping should have non-empty iterator", iterator.hasNext());
        for (int i = 0; i < 7; i++) {
            MyRecord r = iterator.next();
            assertEquals("column index for global mapping should be", -1, r.m_nominalColumnIndex);
            if (r.m_nominalValue.equals("val1")) {
                assertEquals("number value for 'val1' should be", 0, r.m_numberValue);
            } else if (r.m_nominalValue.equals("val2")) {
                assertEquals("number value for 'val2' should be", 1, r.m_numberValue);
            } else if (r.m_nominalValue.equals("val3")) {
                assertEquals("number value for 'val3' should be", 2, r.m_numberValue);
            } else if (r.m_nominalValue.equals("XXXval1")) {
                assertEquals("number value for 'XXXval1' should be", 3, r.m_numberValue);
            } else if (r.m_nominalValue.equals("XXXval2")) {
                assertEquals("number value for 'XXXval2' should be", 4, r.m_numberValue);
            } else if (r.m_nominalValue.equals("YYYval3")) {
                assertEquals("number value for 'YYYval3' should be", 5, r.m_numberValue);
            } else if (r.m_nominalValue.equals("YYYval88")) {
                assertEquals("number value for 'YYYval88' should be", 6, r.m_numberValue);
            } else {
                fail("unexpected nominal value: " + r.m_nominalValue);
            }

        }
    }

}