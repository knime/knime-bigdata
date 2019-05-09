package org.knime.bigdata.spark.local.context;

import java.util.HashMap;
import java.util.Set;

import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;

/**
 * {@link NamedObjectsController} implementation for local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class LocalSparkNamedObjectsController implements NamedObjectsController {

    private final HashMap<String, NamedObjectStatistics> m_statistics;

	private final LocalSparkWrapper m_wrapper;
	
    /**
     * Constructor.
     * 
     * @param wrapper The wrapper for the Spark instance to manage.
     */
    LocalSparkNamedObjectsController(LocalSparkWrapper wrapper) {
    	m_wrapper = wrapper;
        m_statistics = new HashMap<>();
	}

	/**
     * {@inheritDoc}
     */
	@Override
	public Set<String> getNamedObjects() throws KNIMESparkException {
		return m_wrapper.getNamedObjects();
	}

	
    /**
     * {@inheritDoc}
     */
	@Override
    public void deleteNamedObjects(Set<String> namedObjects) throws KNIMESparkException {
        m_wrapper.deleteNamedObjects(namedObjects);
        synchronized (m_statistics) {
            for (final String namedObject : namedObjects) {
                m_statistics.remove(namedObject);
            }
        }
    }

    /**
     * Add {@link NamedObjectStatistics} of object with given name.
     * @param objectName name of object
     * @param statistic statistic of named object
     */
    public void addNamedObjectStatistics(final String objectName, final NamedObjectStatistics statistic) {
        synchronized (m_statistics) {
            m_statistics.put(objectName, statistic);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends NamedObjectStatistics> T getNamedObjectStatistics(final String objectName) {
        synchronized (m_statistics) {
            return (T) m_statistics.get(objectName);
        }
    }
}
