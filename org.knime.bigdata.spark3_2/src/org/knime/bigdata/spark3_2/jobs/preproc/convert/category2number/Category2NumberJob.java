/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark3_2.jobs.preproc.convert.category2number;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import org.knime.bigdata.spark.node.preproc.convert.category2number.NominalValueMappingFactory;
import org.knime.bigdata.spark3_2.api.MappedDatasetContainer;
import org.knime.bigdata.spark3_2.api.NamedObjects;
import org.knime.bigdata.spark3_2.api.RDDUtilsInJava;
import org.knime.bigdata.spark3_2.api.RowBuilder;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public class Category2NumberJob extends AbstractStringMapperJob {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Category2NumberJob.class.getName());

    @Override
    protected Category2NumberJobOutput execute(final SparkContext aContext, final Category2NumberJobInput input, final NamedObjects namedObjects,
            final Dataset<Row> dataset, final int[] colIds) throws KNIMESparkException {

        final MappingType mappingType = input.getMappingType();

        //use only the column indices when converting
        final MappedDatasetContainer mappedData =
            convertNominalValuesForSelectedIndices(dataset, colIds, mappingType, input.keepOriginalCols());

        final String outputName = input.getFirstNamedOutputObject();
        LOGGER.info("Storing mapped data under key: " + outputName);
        namedObjects.addDataFrame(outputName, mappedData.getDatasetWithConvertedValues());

        return new Category2NumberJobOutput(mappedData.getAppendedColumnNames(), mappedData.getMappings());
    }

    /**
     * (Java friendly version) convert nominal values in columns for given column indices to integers and append columns
     * with mapped values
     *
     * @param inputDataset dataset to be processed
     * @param aColumnIds indices of columns to be mapped, columns that have no mapping are ignored
     * @param aMappingType indicates how values are to be mapped
     * @param aKeepOriginalColumns - keep original columns as well or not
     * @note throws SparkException thrown if no mapping is known for some value, but only when row is actually read!
     * @return dataset container with converted data (columns are appended)
     */
    public static MappedDatasetContainer convertNominalValuesForSelectedIndices(final Dataset<Row> inputDataset,
            final int[] aColumnIds, final MappingType aMappingType, final boolean aKeepOriginalColumns) {

        final Set<Integer> inputColIndices = new HashSet<>(aColumnIds.length);
        for (int i : aColumnIds) {
            inputColIndices.add(i);
        }
        final NominalValueMapping mappings = toLabelMapping(inputDataset.javaRDD(), aColumnIds, aMappingType);

        JavaRDD<Row> mappedRdd = inputDataset.javaRDD().map((row) -> {
                final RowBuilder builder;
                if (aKeepOriginalColumns) {
                    builder = RowBuilder.fromRow(row);
                } else {
                    builder = RDDUtilsInJava.dropColumnsFromRow(inputColIndices, row);
                }

                for (int ix : aColumnIds) {
                    //ignore columns that have no mapping
                    if (mappings.hasMappingForColumn(ix)) {
                        Object val = row.get(ix);
                        if (val == null) {
                            if (mappings.getType() == MappingType.BINARY) {
                                int numValues = mappings.getNumberOfValues(ix);
                                for (int i = 0; i < numValues; i++) {
                                    builder.add(0.0d);
                                }
                            } else {
                                builder.add(null);
                            }
                        } else {
                            Integer labelOrIndex = mappings.getNumberForValue(ix, val.toString());
                            if (mappings.getType() == MappingType.BINARY) {
                                int numValues = mappings.getNumberOfValues(ix);
                                for (int i = 0; i < numValues; i++) {
                                    if (labelOrIndex == i) {
                                        builder.add(1.0d);
                                    } else {
                                        builder.add(0.0d);
                                    }
                                }
                            } else {
                                builder.add(labelOrIndex.doubleValue());
                            }
                        }
                    }
                }
                return builder.build();
            });

        return MappedDatasetContainer.createContainer(inputDataset, mappedRdd, aColumnIds, mappings, aKeepOriginalColumns);
    }

    /**
     * extract all distinct values from the given input RDD and the given indices and create a mapping from value to int
     *
     * @param aInputRdd
     * @param aNominalColumnIndices
     * @param aMappingType
     * @return mapping from distinct value to unique integer value
     */
    private static NominalValueMapping toLabelMapping(final JavaRDD<Row> aInputRdd, final int[] aNominalColumnIndices,
        final MappingType aMappingType) {

        switch (aMappingType) {
            case GLOBAL: {
                return toLabelMappingGlobalMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            case COLUMN: {
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            case BINARY: {
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            default: {
                throw new UnsupportedOperationException("ERROR: unknown mapping type !");
            }
        }
    }

    private static NominalValueMapping toLabelMappingGlobalMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices, final MappingType aMappingType) {

        Map<Integer, Set<String>> labels = aggregateValues(aInputRdd, aNominalColumnIndices);

        Map<String, Integer> mappings = new HashMap<>();
        {
            Set<String> allValues = new HashSet<>();
            for (Set<String> labs : labels.values()) {
                allValues.addAll(labs);
            }

            int idx = 0;
            for (String label : allValues) {
                mappings.put(label, idx++);
            }
        }

        Map<Integer, Map<String, Integer>> labelMapping = new HashMap<>(labels.size());
        for (Map.Entry<Integer, Set<String>> entry : labels.entrySet()) {
            Set<String> values = entry.getValue();
            Map<String, Integer> mapping = new HashMap<>(values.size());
            for (String val : values) {
                mapping.put(val, mappings.get(val));
            }
            labelMapping.put(entry.getKey(), mapping);
        }
        return NominalValueMappingFactory.createColumnMapping(labelMapping, aMappingType);
    }

    private static NominalValueMapping toLabelMappingColumnMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices, final MappingType aMappingType) {

        Map<Integer, Set<String>> labels = aggregateValues(aInputRdd, aNominalColumnIndices);

        Map<Integer, Map<String, Integer>> labelMapping = new HashMap<>(labels.size());
        for (Map.Entry<Integer, Set<String>> entry : labels.entrySet()) {
            int idx = 0;
            Set<String> values = entry.getValue();
            Map<String, Integer> mapping = new HashMap<>(values.size());
            for (String val : values) {
                mapping.put(val, idx++);
            }
            labelMapping.put(entry.getKey(), mapping);
        }

        return NominalValueMappingFactory.createColumnMapping(labelMapping, aMappingType);
    }

    /**
     * @param aInputRdd
     * @param aNominalColumnIndices
     * @return
     */
    private static Map<Integer, Set<String>> aggregateValues(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices) {
        Map<Integer, Set<String>> emptyMap = new HashMap<>();
        for (int ix : aNominalColumnIndices) {
            emptyMap.put(ix, new HashSet<String>());
        }

        Map<Integer, Set<String>> labels =
            aInputRdd.aggregate(emptyMap, new Function2<Map<Integer, Set<String>>, Row, Map<Integer, Set<String>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<Integer, Set<String>> call(final Map<Integer, Set<String>> aAggregatedValues, final Row row)
                    throws Exception {
                    for (int ix : aNominalColumnIndices) {
                        Object val = row.get(ix);
                        if (val != null) {
                            //no need to add modified set as the modification is done implicitly
                            aAggregatedValues.get(ix).add(val.toString());
                        }
                    }
                    return aAggregatedValues;
                }
            }, new Function2<Map<Integer, Set<String>>, Map<Integer, Set<String>>, Map<Integer, Set<String>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<Integer, Set<String>> call(final Map<Integer, Set<String>> aAggregatedValues0,
                    final Map<Integer, Set<String>> aAggregatedValues1) throws Exception {
                    for (Map.Entry<Integer, Set<String>> entry : aAggregatedValues0.entrySet()) {
                        entry.getValue().addAll(aAggregatedValues1.get(entry.getKey()));
                    }
                    return aAggregatedValues0;
                }
            });
        return labels;
    }
}
