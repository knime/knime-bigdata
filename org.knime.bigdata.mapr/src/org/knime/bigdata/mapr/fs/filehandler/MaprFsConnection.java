package org.knime.bigdata.mapr.fs.filehandler;

import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.mapr.fs.wrapper.MaprFsConnectionWrapper;
import org.knime.bigdata.mapr.fs.wrapper.MaprFsWrapperFactory;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class MaprFsConnection extends Connection {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(MaprFsConnection.class);

    private final ConnectionInformation m_connectionInformation;

    private final MaprFsConnectionWrapper m_wrapper;

    /**
     * @param connectionInformation the {@link ConnectionInformation}to use
     */
    public MaprFsConnection(final ConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
        m_wrapper = MaprFsWrapperFactory.getConnectionWrapper(m_connectionInformation.useKerberos());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws IOException {
        m_wrapper.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return m_wrapper.isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws IOException {
        m_wrapper.close();
    }

}
