package logia.hibernate.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import logia.hibernate.util.HibernateUtil;
import logia.utility.pool.ObjectPool;

import org.hibernate.Query;
import org.hibernate.Session;

/**
 * The Class AbstractDAO.
 *
 * @author Paul Mai
 * @param <P> the generic type
 * @param <K> the key type
 * @author Paul Mai
 */
public abstract class AbstractDAO<P, K extends Serializable> implements AutoCloseable {

	/**
	 * Delete.
	 *
	 * @param session the session
	 * @param entity the entity want to delete
	 * @throws Exception the exception
	 */
	public void delete(Session session, P entity) throws Exception {
		synchronized (this.getPOJOClass()) {
			session.delete(entity);
		}
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
		synchronized (this.getPOJOClass()) {
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
	}

	/**
	 * Gets the.
	 *
	 * @param session the session
	 * @param key the key
	 * @return the entity
	 */
	@SuppressWarnings("unchecked")
	public P get(Session session, K key) {
		P entity = null;
		try {
			entity = (P) session.get(this.getPOJOClass(), key);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return entity;
	}

	/**
	 * Gets the list entities.
	 *
	 * @param session the session
	 * @return the list entities
	 */
	@SuppressWarnings("unchecked")
	public List<P> getList(Session session) {
		List<P> entities = null;
		try {
			String hql = String.format("Select obj from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			entities = query.list();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return entities;
	}

	/**
	 * Gets list of entities by IDs.
	 *
	 * @param session the session
	 * @param key the key
	 * @return the list entities
	 */
	@SuppressWarnings("unchecked")
	public List<P> getListById(Session session, List<K> key) {
		List<P> entities = new ArrayList<P>();
		try {
			for (K k : key) {
				P entity = (P) session.get(this.getPOJOClass(), k);

				entities.add(entity);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return entities;
	}

	/**
	 * Gets the list id.
	 *
	 * @param session the session
	 * @return the list id
	 */
	@SuppressWarnings("unchecked")
	public List<K> getListId(Session session) {
		List<K> list = null;
		try {
			String hql = String.format("Select id from %s obj", this.getPOJOClass().getName());
			Query query = session.createQuery(hql);
			list = query.list();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
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
		synchronized (this.getPOJOClass()) {
			return (K) session.save(entity);
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
		synchronized (this.getPOJOClass()) {
			session.saveOrUpdate(entity);
		}
	}

	/**
	 * Save or update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws Exception the exception
	 */
	public void saveOrUpdate(Session session, List<P> entities) throws Exception {
		synchronized (this.getPOJOClass()) {
			for (int i = 0; i < entities.size(); i++) {
				P entity = entities.get(i);
				session.saveOrUpdate(entity);
				if (i % HibernateUtil.BATCH_SIZE == 0) {
					session.flush();
					session.clear();
				}
			}
		}
	}

	/**
	 * Select list object by simple query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the list
	 */
	@SuppressWarnings("rawtypes")
	public List selectByQuery(Session session, String queryString) {
		List list = null;
		try {
			Query query = session.createQuery(queryString);
			list = query.list();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Select by sql query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the list
	 */
	@SuppressWarnings("rawtypes")
	public List selectBySqlQuery(Session session, String queryString) {
		List list = null;
		try {
			Query query = session.createSQLQuery(queryString);
			list = query.list();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Select unique object by simple query.
	 *
	 * @param session the session
	 * @param queryString the query string
	 * @return the unique entity
	 */
	@SuppressWarnings("unchecked")
	public P selectUniqueByQuery(Session session, String queryString) {
		P entity = null;
		try {
			Query query = session.createQuery(queryString);
			entity = (P) query.uniqueResult();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return entity;
	}

	/**
	 * Update.
	 *
	 * @param session the session
	 * @param entity the entity want to update
	 * @throws Exception the exception
	 */
	public void update(Session session, P entity) throws Exception {
		synchronized (this.getPOJOClass()) {
			session.update(entity);
		}
	}

	/**
	 * Update list entities.
	 *
	 * @param session the session
	 * @param entities the entities
	 * @throws Exception the exception
	 */
	public void update(Session session, List<P> entities) throws Exception {
		synchronized (this.getPOJOClass()) {
			for (int i = 0; i < entities.size(); i++) {
				P entity = entities.get(i);
				session.update(entity);
				if (i % HibernateUtil.BATCH_SIZE == 0) {
					session.flush();
					session.clear();
				}
			}
		}
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
		synchronized (this.getPOJOClass()) {
			Query query = session.createSQLQuery(queryString);
			return query.executeUpdate();
		}
	}

	/**
	 * Gets the POJO class.
	 * 
	 * @return the POJO class
	 */
	protected abstract Class<P> getPOJOClass();

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

}
