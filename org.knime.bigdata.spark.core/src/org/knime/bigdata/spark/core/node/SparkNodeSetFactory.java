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
package org.knime.bigdata.spark.core.node;

import java.util.Collection;

import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSetFactory;
import org.knime.core.node.config.ConfigRO;

/**
 * {@link NodeSetFactory} implementation of the Spark node. The class looks into the
 * {@link SparkNodeFactoryRegistry} to find all available nodes.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkNodeSetFactory implements NodeSetFactory {

    /**
     *
     */
    private static final String SPARK_CATEGORY = "/toolintegration/spark";

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getNodeFactoryIds() {
        final Collection<String> nodeIds = SparkNodeFactoryRegistry.getNodeIds();
        return nodeIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<? extends NodeFactory<? extends SparkNodeModel>> getNodeFactory(final String id) {
        final SparkNodeFactory<SparkNodeModel> factory = getFactory(id);
        return factory.getNodeFactory();
    }

    private SparkNodeFactory<SparkNodeModel> getFactory(final String id) {
        final SparkNodeFactory<SparkNodeModel> sparkNodeFactory = SparkNodeFactoryRegistry.get(id);
        if (sparkNodeFactory == null) {
            throw new IllegalStateException("No SparkNodeFactory found for id " + id);
        }
        return sparkNodeFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCategoryPath(final String id) {
        final SparkNodeFactory<SparkNodeModel> factory = getFactory(id);
        final StringBuilder path = new StringBuilder(SPARK_CATEGORY);
        final String subPath = factory.getCategoryPath();
        if (!subPath.startsWith("/")) {
            path.append("/");
        }
        path.append(subPath);
        return path.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAfterID(final String id) {
        final SparkNodeFactory<SparkNodeModel> factory = getFactory(id);
        final String afterID = factory.getAfterID();
        //ensure to not return an empty String if the ID is null otherwise the node sorting throws an exception
        return afterID != null ? afterID : "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigRO getAdditionalSettings(final String id) {
        final SparkNodeFactory<SparkNodeModel> factory = getFactory(id);
        return factory.getAdditionalSettings();
    }
}
