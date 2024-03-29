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
package org.knime.bigdata.spark.node.preproc.missingval;

import java.io.IOException;

import org.knime.base.node.preproc.pmml.missingval.MissingCellHandlerFactoryManager;
import org.knime.base.node.preproc.pmml.missingval.utils.MissingValueNodeDescriptionHelper;
import org.knime.bigdata.spark.node.preproc.missingval.handler.DoNothingMissingValueHandlerFactory;
import org.knime.core.node.NodeDescription;
import org.xml.sax.SAXException;

/**
 * Manager for missing spark value handler factories that are provided by extensions.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @since 3.5
 */
public class SparkMissingValueHandlerFactoryManager extends MissingCellHandlerFactoryManager {

    /** The id of the extension point. */
    private static final String EXT_POINT_ID = "org.knime.bigdata.spark.node.SparkMissingValueHandler";

    /** The attribute pointing to the factory class. */
    private static final String EXT_POINT_ATTR_DF = "FactoryClass";

    private static SparkMissingValueHandlerFactoryManager instance;

    /** Cached node description */
    private NodeDescription m_nodeDescription = null;

    /**
     * protected constructor because this class is a singleton.
     *
     * @param extPointId id of the extension point
     * @param extPointAttrDf attribute of the factory class within the handler extension point
     */
    protected SparkMissingValueHandlerFactoryManager(final String extPointId, final String extPointAttrDf) {
        super(extPointId, extPointAttrDf);
    }

    /**
     * Singleton instance of the MissingCellHandlerManager.
     *
     * @return the instance of the MissingCellHandlerManager
     */
    public synchronized static SparkMissingValueHandlerFactoryManager getInstance() {
        if (instance == null) {
            instance = new SparkMissingValueHandlerFactoryManager(EXT_POINT_ID, EXT_POINT_ATTR_DF);
        }
        return instance;
    }

    /**
     * Returns a node description with informations about the missing value handlers of this manager.
     *
     * @param parentDescription node description without handler informations
     * @return node description with handler informations
     * @throws SAXException on failures reading the handler descriptions
     * @throws IOException on failures reading the handler descriptions
     */
    public synchronized NodeDescription getNodeDescription(final NodeDescription parentDescription)
        throws SAXException, IOException {

        if (m_nodeDescription == null) {
            m_nodeDescription = MissingValueNodeDescriptionHelper.createNodeDescription(parentDescription, this);
        }

        return m_nodeDescription;
    }

    @Override
    public String getDoNothingHandlerFactoryId() {
        return DoNothingMissingValueHandlerFactory.getInstance().getID();
    }

    @Override
    public SparkMissingValueHandlerFactory getDoNothingHandlerFactory() {
        return DoNothingMissingValueHandlerFactory.getInstance();
    }
}
