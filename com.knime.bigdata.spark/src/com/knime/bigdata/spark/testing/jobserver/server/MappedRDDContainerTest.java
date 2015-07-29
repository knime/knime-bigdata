package com.knime.bigdata.spark.testing.jobserver.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.MyRecord;
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
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap(), MappingType.COLUMN));
        testObj.createMappingTable(names, offset);

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
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap(), MappingType.GLOBAL));
        testObj.createMappingTable(names, offset);

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
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap(), MappingType.BINARY));
        testObj.createMappingTable(names, offset);

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

    @Test
    public void  convertMappingsToTableAndBack() throws Exception {

        final int offset = 8;
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap(), MappingType.BINARY));
        List<Row> rows = testObj.createMappingTable(names, offset);

        NominalValueMapping mapping = NominalValueMappingFactory.fromTable(rows);

        Iterator<MyRecord> records = mapping.iterator();
        checkRecord(records.next(), "val1", 1, 0);
        checkRecord(records.next(), "val2", 1, 1);
        checkRecord(records.next(), "val3", 1, 2);

        checkRecord(records.next(), "Col2val1", 2, 0);
        checkRecord(records.next(), "Col2val2", 2, 1);
        checkRecord(records.next(), "Col2val3", 2, 2);

        checkRecord(records.next(), "XXXval1", 7, 0);
        checkRecord(records.next(), "XXXval2", 7, 1);
        checkRecord(records.next(), "YYYval3", 7, 2);
        checkRecord(records.next(), "YYYval88", 7, 3);

        assertEquals("mapping type must be extracted from row information", MappingType.BINARY, mapping.getType());
    }

    @Test
    public void  convertMappingsToTableAndBackColumnMapping() throws Exception {

        final int offset = 8;
        MappedRDDContainer testObj = new MappedRDDContainer(null, NominalValueMappingFactory.createColumnMapping(getColumnMappingMap(), MappingType.COLUMN));
        List<Row> rows = testObj.createMappingTable(names, offset);

        NominalValueMapping mapping = NominalValueMappingFactory.fromTable(rows);

        Iterator<MyRecord> records = mapping.iterator();
        checkRecord(records.next(), "val1", 1, 0);
        checkRecord(records.next(), "val2", 1, 1);
        checkRecord(records.next(), "val3", 1, 2);

        checkRecord(records.next(), "Col2val1", 2, 0);
        checkRecord(records.next(), "Col2val2", 2, 1);
        checkRecord(records.next(), "Col2val3", 2, 2);

        checkRecord(records.next(), "XXXval1", 7, 0);
        checkRecord(records.next(), "XXXval2", 7, 1);
        checkRecord(records.next(), "YYYval3", 7, 2);
        checkRecord(records.next(), "YYYval88", 7, 3);

        assertEquals("mapping type must be extracted from row information", MappingType.COLUMN, mapping.getType());
    }

    private void checkRecord(final MyRecord aRecord, final String aNomVal, final int aNomColPos, final int aNumColPos) {
        assertEquals(aNomColPos, aRecord.m_nominalColumnIndex);
        assertEquals("incorrect number value", aNumColPos, aRecord.m_numberValue);
        assertEquals("incorrect nominal value", aNomVal, aRecord.m_nominalValue);
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


}