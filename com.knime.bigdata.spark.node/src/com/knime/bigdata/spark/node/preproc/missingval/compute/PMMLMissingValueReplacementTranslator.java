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
 */
package com.knime.bigdata.spark.node.preproc.missingval.compute;

import java.util.Map;

import org.apache.xmlbeans.SchemaType;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.PMMLDocument;
import org.dmg.pmml.TransformationDictionaryDocument.TransformationDictionary;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;
import org.knime.core.node.port.pmml.PMMLTranslator;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;

import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;

/**
 * PMML translator using Spark missing value handlers.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
public class PMMLMissingValueReplacementTranslator implements PMMLTranslator {

    private final SparkMissingValueHandler[] m_handlers;

    private final Map<String, Object> m_aggResults;

    /**
     * Constructor for a PMMLMissingValueReplacementTranslator.
     * 
     * @param handlers the handlers that were used to replace missing values
     * @param aggResults result of aggregation handlers
     */
    public PMMLMissingValueReplacementTranslator(final SparkMissingValueHandler[] handlers,
        final Map<String, Object> aggResults) {
        m_handlers = handlers;
        m_aggResults = aggResults;
    }

    @Override
    public void initializeFrom(final PMMLDocument pmmlDoc) {

    }

    @Override
    public SchemaType exportTo(final PMMLDocument pmmlDoc, final PMMLPortObjectSpec spec) {
        TransformationDictionary td = pmmlDoc.getPMML().getTransformationDictionary();
        if (td == null) {
            td = pmmlDoc.getPMML().addNewTransformationDictionary();
        }
        DerivedFieldMapper mapper = new DerivedFieldMapper(pmmlDoc);

        for (SparkMissingValueHandler handler : m_handlers) {
            DerivedField f = handler.getPMMLDerivedField(m_aggResults.get(handler.getColumnSpec().getName()));
            if (f != null) {
                f.setDisplayName(f.getName());
                f.setName(mapper.createDerivedFieldName(f.getName()));
                td.getDerivedFieldList().add(f);
            }
        }

        return null;
    }

}
