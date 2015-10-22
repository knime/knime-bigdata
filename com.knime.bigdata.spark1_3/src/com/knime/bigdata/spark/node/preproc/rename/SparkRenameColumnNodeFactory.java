package com.knime.bigdata.spark.node.preproc.rename;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkRenameColumnNodeFactory
        extends NodeFactory<SparkRenameColumnNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkRenameColumnNodeModel createNodeModel() {
        return new SparkRenameColumnNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<SparkRenameColumnNodeModel> createNodeView(final int viewIndex,
            final SparkRenameColumnNodeModel nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new SparkRenameColumnNodeDialog();
    }

}

