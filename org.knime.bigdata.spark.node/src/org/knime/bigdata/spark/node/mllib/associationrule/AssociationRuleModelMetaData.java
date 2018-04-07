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
 *   Created on Feb 14, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.associationrule;

import java.io.Serializable;

import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.core.data.DataType;

/**
 * Meta data of the association rules model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleModelMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Itemset type in the rules */
    private IntermediateDataType m_itemsetType;

    /** Deserialization constructor */
    public AssociationRuleModelMetaData() {}

    /**
     * Default constructor.
     * @param itemsetType itemset type
     */
    public AssociationRuleModelMetaData(final DataType itemsetType) {
        m_itemsetType = KNIMEToIntermediateConverterRegistry.get(itemsetType).getIntermediateDataType();
    }

    /** @return Type of items in the rules */
    public DataType getItemsetType() {
        return KNIMEToIntermediateConverterRegistry.get(m_itemsetType).getKNIMEDataType();
    }

    /** @return Short summary of the meta data */
    public String[] getSummary() {
        return new String[] { "Items Type: " + getItemsetType().getCollectionElementType() };
    }
}
