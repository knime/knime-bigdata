package org.knime.bigdata.spark.local.context;

import java.util.Set;

import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;

/**
 * {@link NamedObjectsController} implementation for local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class LocalSparkNamedObjectsController implements NamedObjectsController {

	private final LocalSparkWrapper m_wrapper;
	
    /**
     * Constructor.
     * 
     * @param wrapper The wrapper for the Spark instance to manage.
     */
    LocalSparkNamedObjectsController(LocalSparkWrapper wrapper) {
    	m_wrapper = wrapper;
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
	}
}
