package com.knime.bigdata.spark.testing.jobserver.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.NominalValueMappingFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class MappedRDDContainerTest {

    private final static Map<Integer, String> names = new HashMap<>();
    static {
        names.put(1, "Col1");
        names.put(2, "Col2");
        names.put(7, "Col7");
    }



    @Test
    public void columnMappingShouldGenerateSensibleColumnNames() throws Exception {

        final int offset = 8;
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap()));
        testObj.createMappingTable(names, MappingType.COLUMN, offset);

        for (Entry<Integer, String> entry : testObj.getColumnNames().entrySet()) {
            final String n;
            if (names.containsKey(entry.getKey())) {
                n = names.get(entry.getKey());
            } else if (entry.getKey() == offset) {
                n = "Col1"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else if (entry.getKey() == offset+1) {
                n = "Col2"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else if (entry.getKey() == offset+2) {
                n = "Col7"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else {
                n = null;
            }
            assertEquals("incorrect column name, ", n, entry.getValue());
        }
    }

    @Test
    public void columnMappingShouldGenerateSensibleColumnNamesForGlobalMapping() throws Exception {

        final int offset = 8;
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap()));
        testObj.createMappingTable(names, MappingType.GLOBAL, offset);

        for (Entry<Integer, String> entry : testObj.getColumnNames().entrySet()) {
            final String n;
            if (names.containsKey(entry.getKey())) {
                n = names.get(entry.getKey());
            } else if (entry.getKey() == offset) {
                n = "Col1"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else if (entry.getKey() == offset+1) {
                n = "Col2"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else if (entry.getKey() == offset+2) {
                n = "Col7"+NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
            } else {
                n = null;
            }
            assertEquals("incorrect column name, ", n, entry.getValue());
        }
    }

    @Test
    public void columnMappingShouldGenerateSensibleColumnNamesForBinaryMappings() throws Exception {

        final int offset = 8;
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap()));
        testObj.createMappingTable(names, MappingType.BINARY, offset);

        for (Entry<Integer, String> entry : testObj.getColumnNames().entrySet()) {

            if (names.containsKey(entry.getKey())) {
                final String n = names.get(entry.getKey());
                assertEquals("incorrect column name, ", n, entry.getValue());
            } else if (entry.getKey() == 8) {
                assertEquals("value 0 of Col1 must be added first", "Col1_val1", entry.getValue());
            } else if (entry.getKey() == 9) {
                assertEquals("value 1 of Col1 must be added second", "Col1_val2", entry.getValue());
            } else if (entry.getKey() == 10) {
                assertEquals("value 2 of Col1 must be added third", "Col1_val3", entry.getValue());
            } else if (entry.getKey() == 11) {
                assertEquals("values of Col2 must be added after values of Col1 ", "Col2_Col2val1", entry.getValue());
            } else if (entry.getKey() > 10 && entry.getKey() < 14) {
                assertTrue("values of Col2 must be added second", entry.getValue().startsWith("Col2_Col2val"));
            } else if (entry.getKey() > 13 && entry.getKey() < 18) {
                assertTrue("values of Col7 must be added LAST", entry.getValue().startsWith("Col7_"));
            } else {
                fail("unexpected column index: "+entry.getKey());
            }
        }
    }


    /**
     * @return
     */
    private Map<Integer, Map<String, Integer>> getColumnMappingMap() {
        final Map<Integer, Map<String, Integer>> mapping = new HashMap<Integer, Map<String, Integer>>();
        {
            final Map<String, Integer> colMapping = new HashMap<>();
            colMapping.put("val1", 0);
            colMapping.put("val3", 2);
            colMapping.put("val2", 1);

            mapping.put(1, colMapping);
        }

        {
            final Map<String, Integer> colMapping = new HashMap<>();
            colMapping.put("Col2val2", 1);
            colMapping.put("Col2val3", 2);
            colMapping.put("Col2val1", 0);

            mapping.put(2, colMapping);
        }
        {
            final Map<String, Integer> colMapping = new HashMap<>();
            colMapping.put("XXXval1", 0);
            colMapping.put("YYYval88", 3);
            colMapping.put("XXXval2", 1);
            colMapping.put("YYYval3", 2);
            mapping.put(7, colMapping);
        }
        return mapping;
    }

//    /**
//     * @return
//     */
//    private Map<String, Integer> getGlobalMappingMap() {
//        final Map<String, Integer> mapping = new HashMap<>();
//
//        mapping.put("val1", 0);
//        mapping.put("val2", 1);
//        mapping.put("val3", 2);
//
//        mapping.put("XXXval1", 3);
//        mapping.put("XXXval2", 4);
//        mapping.put("YYYval3", 5);
//        mapping.put("YYYval88", 6);
//        return mapping;
//    }

}