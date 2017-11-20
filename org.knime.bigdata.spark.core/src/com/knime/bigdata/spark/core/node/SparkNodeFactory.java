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
 *   Created on 28.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.node;

import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;

/**
 * Interface that needs to be implemented by all Spark node provider. One could also use the default implementation
 * {@link DefaultSparkNodeFactory}.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> {@link SparkNodeModel} implementation
 */
public interface SparkNodeFactory<M extends SparkNodeModel> {

    /**
     * @return the unique node factory id
     */
    public String getId();

    /**
     * @return the node factory
     */
    public Class<? extends NodeFactory<M>> getNodeFactory();

    /**
     * @return the category the node associated with this node factory belongs
     *         to
     */
    public String getCategoryPath();

    /**
     * @return the ID after which this factory's node is sorted in or an empty {@link String}
     */
    public String getAfterID();

    /**
     * @return additional settings for the node factory
     */
    public ConfigRO getAdditionalSettings();

}
