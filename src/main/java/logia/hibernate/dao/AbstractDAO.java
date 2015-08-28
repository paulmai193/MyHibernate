package logia.hibernate.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import logia.hibernate.util.HibernateUtil;
import logia.utility.pool.ObjectPool;

import org.apache.log4j.Logger;
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
public abstract class AbstractDAO<P, K extends Serializable> implements AutoCloseable {

	/** The logger. */
	protected final Logger LOGGER = Logger.getLogger(this.getClass());

	/**
	 * Borrow from pool.
	 *
	 * @param <T> the generic type
	 * @param pool the pool
	 * @return the t
	 */
	public static <T extends AbstractDAO<?, ?>> T borrowFromPool(ObjectPool<T> pool) {
		return pool.borrowObject();
	}

	/**
	 * Delete.
	 *
	 * @param session the session
	 * @param entity the entity want to delete
	 * @throws Exception the exception
	 */
	public void delete(Session session, P entity) throws Exception {
		session.delete(entity);
	}

	/**
	 * Delete list.
	 *
	 * @param session the session
	 * @param listId the list ID
	 * @return the number of entities deleted.
	 * @throws Exception the exception
	 */
	public int deleteList(Session session, List<K> listId) throws Exception {
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
	 * @param session the session
	 * @param key the key
	 * @return the entity
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public P get(Session session, K key) throws Exception {
		return (P) session.get(this.getPOJOClass(), key);
	}

	/**
	 * Gets the list entities.
	 *
	 * @param session the session
	 * @return the list entities
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> getList(Session session) throws Exception {
		List<P> entities = null;
		String hql = String.format("Select obj from %s obj", this.getPOJOClass().getName());
		Query query = session.createQuery(hql);
		entities = query.list();
		return entities;
	}

	/**
	 * Gets the list.
	 *
	 * @param session the session
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public List<P> getList(Session session, int limit, int offset) throws Exception {
		List<P> entities = null;
		String hql = String.format("Select obj from %s obj", this.getPOJOClass().getName());
		Query query = session.createQuery(hql);
		query.setMaxResults(limit);
		query.setFirstResult(offset);
		entities = query.list();
		return entities;
	}

	/**
	 * Gets list of entities by IDs.
	 *
	 * @param session the session
	 * @param key the key
	 * @return the list entities
	 * @throws Exception the exception
	 */
	public List<P> getListById(Session session, List<K> key) throws Exception {
		List<P> entities = new ArrayList<P>();
		for (K k : key) {
			@SuppressWarnings("unchecked")
			P entity = (P) session.get(this.getPOJOClass(), k);

			entities.add(entity);
		}
		return entities;
	}

	/**
	 * Gets the list id.
	 *
	 * @param session the session
	 * @return the list id
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public List<K> getListId(Session session) throws Exception {
		List<K> list = null;
		String hql = String.format("Select id from %s obj", this.getPOJOClass().getName());
		Query query = session.createQuery(hql);
		list = query.list();
		return list;
	}

	/**
	 * Gets the list id.
	 *
	 * @param session the session
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list id
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public List<K> getListId(Session session, int limit, int offset) throws Exception {
		List<K> list = null;
		String hql = String.format("Select id from %s obj", this.getPOJOClass().getName());
		Query query = session.createQuery(hql);
		query.setMaxResults(limit);
		query.setFirstResult(offset);
		list = query.list();
		return list;
	}

	/**
	 * Save an new entity and return ID.
	 *
	 * @param session the session
	 * @param entity the entity
	 * @return the ID of entity
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public K saveID(Session session, P entity) throws Exception {
		return (K) session.save(entity);
	}

	/**
	 * Save or update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws Exception the exception
	 */
	public void saveOrUpdate(Session session, List<P> entities) throws Exception {
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
	 * @throws Exception the exception
	 */
	public void saveOrUpdate(Session session, P entity) throws Exception {
		session.saveOrUpdate(entity);
	}

	/**
	 * Select list object by simple query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the list
	 * @throws Exception the exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectByQuery(Session session, String queryString) throws Exception {
		List list = null;
		Query query = session.createQuery(queryString);
		list = query.list();
		return list;
	}

	/**
	 * Select by query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws Exception the exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectByQuery(Session session, String queryString, int limit, int offset) throws Exception {
		List list = null;
		Query query = session.createQuery(queryString);
		query.setMaxResults(limit);
		query.setFirstResult(offset);
		list = query.list();
		return list;
	}

	/**
	 * Select by sql query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the list
	 * @throws Exception the exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(Session session, String queryString) throws Exception {
		List list = null;
		Query query = session.createSQLQuery(queryString);
		list = query.list();
		return list;
	}

	/**
	 * Select by sql query.
	 *
	 * @param session the session
	 * @param queryString the SQL query string
	 * @param limit the maximum number of rows
	 * @param offset the first result a row number, start from 0
	 * @return the list
	 * @throws Exception the exception
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(Session session, String queryString, int limit, int offset) throws Exception {
		List list = null;
		Query query = session.createSQLQuery(queryString);
		query.setMaxResults(limit);
		query.setFirstResult(offset);
		list = query.list();
		return list;
	}

	/**
	 * Select unique object by simple query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the unique entity
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public P selectUniqueByQuery(Session session, String queryString) throws Exception {
		P entity = null;
		Query query = session.createQuery(queryString);
		entity = (P) query.uniqueResult();
		return entity;
	}

	/**
	 * Update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws Exception the exception
	 */
	public void update(Session session, List<P> entities) throws Exception {
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
	 * @throws Exception the exception
	 */
	public void update(Session session, P entity) throws Exception {
		session.update(entity);
	}

	/**
	 * Update by sql query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the number entity updated or deleted
	 * @throws Exception the exception
	 */
	public int updateBySQLQuery(Session session, String queryString) throws Exception {
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
