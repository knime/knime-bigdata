package org.knime.bigdata.mapr.fs.node.connector;

import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeDialog;
import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeModel;
import org.knime.bigdata.mapr.fs.filehandler.MaprFsRemoteFileHandler;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for node.
 *
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class MaprFsConnectionInformationNodeFactory extends NodeFactory<ConnectionInformationNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectionInformationNodeModel createNodeModel() {
        return new MaprFsConnectionInformationNodeModel();
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
    public NodeView<ConnectionInformationNodeModel> createNodeView(final int viewIndex,
            final ConnectionInformationNodeModel nodeModel) {
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
        return new ConnectionInformationNodeDialog(MaprFsRemoteFileHandler.MAPRFS_PROTOCOL);
    }
}
