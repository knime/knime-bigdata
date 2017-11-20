package org.knime.bigdata.spark.node.preproc.rename;

import org.knime.core.node.NodeDialogPane;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkRenameColumnNodeFactory extends DefaultSparkNodeFactory<SparkRenameColumnNodeModel> {

    /**
     * Constructor.
     */
    public SparkRenameColumnNodeFactory() {
        super("column/convert");
    }
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

