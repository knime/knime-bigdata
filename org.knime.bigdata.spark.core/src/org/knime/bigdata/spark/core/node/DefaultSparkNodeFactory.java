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

import org.knime.core.node.DynamicNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.config.ConfigRO;

/**
 * Default implementation of the {@link SparkNodeFactory} interface.
 * @author Tobias Koetter, KNIME.com
 * @param <M> {@link SparkNodeModel} implementation
 */
public abstract class DefaultSparkNodeFactory<M extends SparkNodeModel> extends DynamicNodeFactory<M>
implements SparkNodeFactory<M> {


    private String m_id;
    private String m_categoryPath;
    private String m_afterID;
    private ConfigRO m_additionalSettings;

    /**
     * @param categoryPath the category path the node should be displayed starting with the Spark category e.g.
     * node repository Spark/IO/Writer/MyNode results in a categoryPath of IO/Writer
     */
    public DefaultSparkNodeFactory(final String categoryPath) {
        this(null, categoryPath, null, null);
    }

    /**
     * @param id unique node id
     * @param categoryPath the category path the node should be displayed starting with the Spark category e.g.
     * node repository Spark/IO/Writer/MyNode results in a categoryPath of IO/Writer
     */
    public DefaultSparkNodeFactory(final String id, final String categoryPath) {
        this(id, categoryPath, null, null);
    }

    /**
     * @param id unique node id
     * @param categoryPath the category path the node should be displayed starting with the Spark category e.g.
     * node repository Spark/IO/Writer/MyNode results in a categoryPath of /IO/Writer
     * @param afterID the id of the node this node should be displayed after
     * @param additionalSettings the additional settings as {@link ConfigRO}
     */
    public DefaultSparkNodeFactory(final String id, final String categoryPath, final String afterID,
        final ConfigRO additionalSettings) {
        if (id == null) {
            //if no id is given use the class name
            m_id = getClass().getName();
        } else {
            m_id = id;
        }
        m_categoryPath = categoryPath;
        m_afterID = afterID != null ? afterID : "";
        m_additionalSettings = additionalSettings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return m_id;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends DynamicNodeFactory<M>> getNodeFactory() {
        return (Class<? extends DynamicNodeFactory<M>>)getClass();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCategoryPath() {
        return m_categoryPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAfterID() {
        return m_afterID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConfigRO getAdditionalSettings() {
        return m_additionalSettings;
    }

    /**
     * Spark nodes with a view need to overwrite this method.
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    /**
     * Spark nodes with a view need to overwrite this method.
     * {@inheritDoc}
     */
    @Override
    public NodeView<M> createNodeView(final int viewIndex, final M nodeModel) {
        return null;
    }

    /**
     * Spark nodes with a dialog need to overwrite this method.
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return false;
    }

    /**
     * Spark nodes with a dialog need to overwrite this method.
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return null;
    }
    /**
     * Looks for a XML file with the name of the factory class within the same page. Overwrite this method if you
     * want to provide another {@link NodeDescription}.
     * {@inheritDoc}
     */
    @Override
    protected NodeDescription createNodeDescription() {
        return parseNodeDescriptionFromFile();
    }
}
