package logia.hibernate.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import logia.hibernate.util.HibernateUtil;
import logia.utility.pool.ObjectPool;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.NonUniqueResultException;
import org.hibernate.Query;
import org.hibernate.Session;

/**
 * The Class AbstractDAO.
 *
 * @author Paul Mai
 * @author Paul Mai
 * @param <P> the generic type
 * @param <K> the key type
 */
public abstract class AbstractDAO<P, K extends Serializable> {

	/** The logger. */
	protected final Logger LOGGER = Logger.getLogger(this.getClass());

	protected AbstractDAO() {
	}

	/**
	 * Borrow from pool.
	 *
	 * @param <T> the generic type
	 * @param pool the pool
	 * @return the t
	 */
	@Deprecated
	public static <T extends AbstractDAO<?, ?>> T borrowFromPool(ObjectPool<T> pool) {
		return pool.borrowObject();
	}

	/**
	 * Delete.
	 *
	 * @param session the session
	 * @param entity the entity want to delete
	 * @throws HibernateException the hibernate exception
	 */
	public void delete(Session session, P entity) throws HibernateException {
		session.delete(entity);
	}

	/**
	 * Delete list.
	 *
	 * @param session the session
	 * @param listId the list ID
	 * @return the number of entities deleted.
	 * @throws HibernateException the hibernate exception
	 */
	public int deleteList(Session session, List<K> listId) throws HibernateException {
		int numberEffect;
		if (listId.size() > 0) {
			Query query = session.createQuery(String.format("delete from %s obj where id in (:idList)", this.getPOJOClass().getName()));
			query.setParameterList("idList", listId);
			numberEffect = query.executeUpdate();
		}
		else {
			numberEffect = 0;
		}
		return numberEffect;
	}

	/**
	 * Gets the.
	 *
	 * @param key the key
	 * @return the entity
	 * @throws Exception the exception
	 */
	public P get(K key) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			return (P) session.get(this.getPOJOClass(), key);
		}
		finally {
			HibernateUtil.closeSession(session);
		}

	}

	/**
	 * Gets the list entities.
	 *
	 * @return the list entities
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> getList() throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			String hql = String.format("Select obj from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Gets the list.
	 *
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> getList(int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			String hql = String.format("Select obj from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Gets list of entities by IDs.
	 *
	 * @param key the key
	 * @return the list entities
	 * @throws HibernateException the hibernate exception
	 */
	public List<P> getListById(List<K> key) throws HibernateException {
		List<P> entities = new ArrayList<P>();
		Session session = HibernateUtil.getSession();
		try {
			for (K k : key) {
				P entity = (P) session.get(this.getPOJOClass(), k);

				entities.add(entity);
			}
			return entities;
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Gets the list id.
	 *
	 * @return the list id
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<K> getListId() throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			String hql = String.format("Select id from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Gets the list id.
	 *
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list id
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<K> getListId(int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			String hql = String.format("Select id from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Save an new entity and return ID.
	 *
	 * @param session the session
	 * @param entity the entity
	 * @return the ID of entity
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public K saveID(Session session, P entity) throws HibernateException {
		return (K) session.save(entity);
	}

	/**
	 * Save or update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws HibernateException the hibernate exception
	 */
	public void saveOrUpdate(Session session, List<P> entities) throws HibernateException {
		for (int i = 0; i < entities.size(); i++) {
			P entity = entities.get(i);
			session.saveOrUpdate(entity);
			if (i % HibernateUtil.BATCH_SIZE == 0) {
				session.flush();
				session.clear();
			}
		}
	}

	/**
	 * Save or update.
	 *
	 * @param session the session
	 * @param entity the entity
	 * @throws HibernateException the hibernate exception
	 */
	public void saveOrUpdate(Session session, P entity) throws HibernateException {
		session.saveOrUpdate(entity);
	}

	/**
	 * Select list object by simple query.
	 *
	 * @param queryString the query string
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	public List<P> selectByQuery(String queryString) throws HibernateException {
		return selectByQuery(queryString, -1, 0);
	}

	/**
	 * Select by query.
	 *
	 * @param parameters the map contain parameters of query string
	 * @param parameters the parameters
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	public List<P> selectByQuery(String queryString, Map<String, Object> parameters) throws HibernateException {
		return selectByQuery(queryString, parameters, -1, 0);
	}

	/**
	 * Select by query.
	 *
	 * @param queryString the query string
	 * @param limit the maximum number of rows. Zero or negatives value as meaning no limit...
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> selectByQuery(String queryString, int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			Query query = session.createQuery(queryString);
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Select by query.
	 *
	 * @param queryString the query string
	 * @param parameters the map contain parameters of query string
	 * @param limit the maximum number of rows. Zero or negatives value as meaning no limit...
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> selectByQuery(String queryString, Map<String, Object> parameters, int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			Query query = session.createQuery(queryString);
			for (Entry<String, Object> parameter : parameters.entrySet()) {
				query.setParameter(parameter.getKey(), parameter.getValue());
			}
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Select by sql query.
	 *
	 * @param sqlQueryString the sql query string
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(String sqlQueryString) throws HibernateException {
		return selectBySqlQuery(sqlQueryString, -1, 0);
	}

	/**
	 * Select by sql query.
	 *
	 * @param sqlQueryString the sql query string
	 * @param parameters the map contain parameters of query string
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(String sqlQueryString, Map<String, Object> parameters) throws HibernateException {
		return selectBySqlQuery(sqlQueryString, parameters, -1, 0);
	}

	/**
	 * Select by sql query.
	 *
	 * @param sqlQueryString the sql query string
	 * @param limit the maximum number of rows. Zero or negatives value as meaning no limit...
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(String sqlQueryString, int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			Query query = session.createSQLQuery(sqlQueryString);
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Select by sql query.
	 *
	 * @param sqlQueryString the sql query string
	 * @param parameters the map contain parameters of query string
	 * @param limit the maximum number of rows. Zero or negatives value as meaning no limit...
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(String sqlQueryString, Map<String, Object> parameters, int limit, int offset) throws HibernateException {
		Session session = HibernateUtil.getSession();
		try {
			Query query = session.createSQLQuery(sqlQueryString);
			for (Entry<String, Object> parameter : parameters.entrySet()) {
				query.setParameter(parameter.getKey(), parameter.getValue());
			}
			query.setMaxResults(limit);
			query.setFirstResult(offset);
			return query.list();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Select unique object by simple query.
	 *
	 * @param queryString the query string
	 * @return the unique entity
	 * @throws HibernateException the hibernate exception
	 */
	@SuppressWarnings("unchecked")
	public P selectUniqueByQuery(String queryString) throws HibernateException, NonUniqueResultException {
		Session session = HibernateUtil.getSession();
		try {
			Query query = session.createQuery(queryString);
			return (P) query.uniqueResult();
		}
		finally {
			HibernateUtil.closeSession(session);
		}
	}

	/**
	 * Update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws HibernateException the hibernate exception
	 */
	public void update(Session session, List<P> entities) throws HibernateException {
		for (int i = 0; i < entities.size(); i++) {
			P entity = entities.get(i);
			session.update(entity);
			if (i % HibernateUtil.BATCH_SIZE == 0) {
				session.flush();
				session.clear();
			}
		}
	}

	/**
	 * Update.
	 *
	 * @param session the session
	 * @param entity the entity want to update
	 * @throws HibernateException the hibernate exception
	 */
	public void update(Session session, P entity) throws HibernateException {
		session.update(entity);
	}

	/**
	 * Update by sql query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the number entity updated or deleted
	 * @throws HibernateException the hibernate exception
	 */
	public int updateBySQLQuery(Session session, String queryString) throws HibernateException {
		Query query = session.createSQLQuery(queryString);
		return query.executeUpdate();
	}

	/**
	 * Gets the POJO class.
	 * 
	 * @return the POJO class
	 */
	protected abstract Class<P> getPOJOClass();

}
