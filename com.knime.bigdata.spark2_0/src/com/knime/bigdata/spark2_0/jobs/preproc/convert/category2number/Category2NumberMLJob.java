/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark2_0.jobs.preproc.convert.category2number;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.ColumnPruner;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import com.knime.bigdata.spark.node.preproc.convert.MyRecord;
import com.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SupervisedLearnerUtils;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public class Category2NumberMLJob extends AbstractStringMapperJob {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(Category2NumberMLJob.class.getName());

    @Override
    protected Category2NumberJobOutput execute(final SparkContext aContext, final Category2NumberJobInput input,
        final NamedObjects namedObjects, final Dataset<Row> dataset, final int[] colIds) throws KNIMESparkException {

        final List<PipelineStage> pipelineStages = new ArrayList<>();

        final MappingType mappingType = input.getMappingType();

        final String[] colNames = dataset.columns();
        final List<String> appendedColumnNames = new ArrayList<>();
        final Map<Integer, StringIndexerModel> indexers = new HashMap<>();
        for (Integer colIndex : colIds) {
            String nominalColName = colNames[colIndex];
            if (mappingType == MappingType.COLUMN) {
                appendedColumnNames
                    .add(pipelineAsIntMapping(colIndex, nominalColName, indexers, pipelineStages, dataset));
            } else if (mappingType == MappingType.BINARY) {
                appendedColumnNames.addAll(SupervisedLearnerUtils.addToPipelineAsOneHotEncodingMapping(colIndex,
                    nominalColName, input.isDropLast(), indexers, pipelineStages, dataset, true));
            } else {
                throw new RuntimeException("Unsupported mapping type: " + mappingType);
            }

            if (!input.keepOriginalCols()) {
                pipelineStages.add(new ColumnPruner(new scala.collection.immutable.Set.Set1<String>(nominalColName)));
            }
        }

        final String outputName = input.getFirstNamedOutputObject();
        LOGGER.info("Storing mapped data under key: " + outputName);
        final Pipeline pipeline =
            new Pipeline().setStages(pipelineStages.toArray(new PipelineStage[pipelineStages.size()]));
        final PipelineModel model = pipeline.fit(dataset);

        namedObjects.addDataFrame(outputName, model.transform(dataset));

        NominalValueMapping mapping = new Category2NumberNominalValueMapping(indexers, mappingType, colIds, input.isDropLast());

        return new Category2NumberJobOutput(appendedColumnNames.toArray(new String[appendedColumnNames.size()]),
            mapping);
    }

    /**
     * @param colIndex
     * @param nominalColName
     * @param appendedColumnNames
     * @param indexers
     * @param pipelineStages
     * @param dataset
     *
     */
    private static String pipelineAsIntMapping(final Integer colIndex, final String nominalColName,
        final Map<Integer, StringIndexerModel> indexers, final List<PipelineStage> pipelineStages,
        final Dataset<Row> dataset) {
        final String newNominalColName = nominalColName + NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;

        final StringIndexerModel indexer =
            new StringIndexer().setInputCol(nominalColName).setOutputCol(newNominalColName).fit(dataset);
        pipelineStages.add(indexer);
        indexers.put(colIndex, indexer);
        return newNominalColName;
    }

    static class Category2NumberNominalValueMapping implements NominalValueMapping {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        private final Set<Integer> mappedColumnIndices = new HashSet<>();

        private final MappingType m_mappingType;

        private final Map<Integer, StringIndexerModel> m_indexers;

        private final List<MyRecord> m_records = new ArrayList<>();

        /**
         * @param aIndexers
         * @param aMappingType
         *
         */
        Category2NumberNominalValueMapping(final Map<Integer, StringIndexerModel> aIndexers,
            final MappingType aMappingType, final int[] colIds, final boolean aDropLast) {
            m_indexers = aIndexers;
            for (int colId : colIds) {
                mappedColumnIndices.add(colId);
            }
            m_mappingType = aMappingType;

            for (Entry<Integer, StringIndexerModel> entry : m_indexers.entrySet()) {
                Integer colIx = entry.getKey();
                StringIndexerModel indexer = entry.getValue();

                String[] labels = indexer.labels();
                for (int ix = 0; ix < labels.length; ix++) {
                    if (!aDropLast || ix < labels.length - 1) {
                        m_records.add(new MyRecord(colIx, labels[ix], ix));
                    }
                }
            }
        }

        @Override
        public int size() {
            return mappedColumnIndices.size();
        }

        @Override
        public Iterator<MyRecord> iterator() {
            return m_records.iterator();
        }

        @Override
        public boolean hasMappingForColumn(final int aNominalColumnIx) {
            return mappedColumnIndices.contains(aNominalColumnIx);
        }

        @Override
        public MappingType getType() {
            return m_mappingType;
        }

        @Override
        public int getNumberOfValues(final int aNominalColumnIx) {
            if (m_indexers.containsKey(aNominalColumnIx)) {
                return m_indexers.get(aNominalColumnIx).labels().length;
            }
            return 0;
        }

        @Override
        public Integer getNumberForValue(final int aColumnIx, final String aValue) throws NoSuchElementException {
            if (m_indexers.containsKey(aColumnIx)) {
                String[] labels = m_indexers.get(aColumnIx).labels();
                for (int ix = 0; ix < labels.length; ix++) {
                    if (labels[ix].equals(aValue)) {
                        return ix;
                    }
                }
            }
            return null;
        }
    }
}
