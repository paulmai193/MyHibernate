package logia.hibernate.util;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.service.ServiceRegistry;

/**
 * Hibernate Utility class with a convenient method to get Session Factory object.
 * 
 * @author Paul Mai
 */
public class HibernateUtil {

	/** The batch size. */
	public static int              BATCH_SIZE;

	/** The config file. */
	private static String          _configFile = "hibernate-sample.cfg.xml";

	/** The configuration. */
	private static Configuration   _configuration;

	/** The _service registry. */
	private static ServiceRegistry _serviceRegistry;

	/** The _session factory. */
	private static SessionFactory  _sessionFactory;

	/**
	 * Create new session and begin transaction.
	 * 
	 * @return the session
	 */
	public static Session beginTransaction() {
		Session hibernateSession = HibernateUtil.getSession();
		hibernateSession.beginTransaction();
		return hibernateSession;
	}

	/**
	 * Build session factory.
	 */
	public static void buildSessionFactory() {
		try {
			HibernateUtil._configuration = new Configuration();
			HibernateUtil._configuration.configure(HibernateUtil._configFile);

			String proBatchSize = HibernateUtil._configuration.getProperty("hibernate.jdbc.batch_size");
			try {
				HibernateUtil.BATCH_SIZE = Integer.parseInt(proBatchSize);
			}
			catch (Exception e) {
				HibernateUtil.BATCH_SIZE = 20;
			}

			HibernateUtil._serviceRegistry = new StandardServiceRegistryBuilder().applySettings(HibernateUtil._configuration.getProperties()).build();
			HibernateUtil._sessionFactory = HibernateUtil._configuration.buildSessionFactory(HibernateUtil._serviceRegistry);
			System.out.println("Build session factory success");

		}
		catch (HibernateException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Close the input session.
	 * 
	 * @param hibernateSession the hibernate session
	 */
	public static void closeSession(Session hibernateSession) {
		hibernateSession.close();
		hibernateSession = null;
	}

	/**
	 * Commit transaction of the input session.
	 * 
	 * @param hibernateSession the hibernate session
	 */
	public static void commitTransaction(Session hibernateSession) {
		hibernateSession.getTransaction().commit();
	}

	/**
	 * Gets the full text session.
	 *
	 * @return the full text session
	 */
	public static FullTextSession getFullTextSession() {
		Session session = HibernateUtil.getSession();
		FullTextSession fullTextSession = Search.getFullTextSession(session);
		return fullTextSession;
	}

	/**
	 * Gets the session.
	 *
	 * @return the session
	 */
	public static Session getSession() {
		if (HibernateUtil._sessionFactory == null || HibernateUtil._sessionFactory.isClosed()) {
			HibernateUtil.buildSessionFactory();
		}
		Session hibernateSession = HibernateUtil._sessionFactory.openSession();
		return hibernateSession;
	}

	/**
	 * Indexing for Hibernate search.
	 *
	 * @throws Exception the exception
	 */
	public static synchronized void indexing() throws Exception {
		FullTextSession fullTextSession = HibernateUtil.getFullTextSession();
		fullTextSession.createIndexer().startAndWait();
		HibernateUtil.closeSession(fullTextSession);
	}

	/**
	 * Release hibernate session factory.
	 */
	public static void releaseFactory() {
		try {
			// if (HibernateUtil._sessionFactory instanceof SessionFactoryImpl) {
			// SessionFactoryImpl impl = (SessionFactoryImpl) HibernateUtil._sessionFactory;
			// ConnectionProvider connectionProvider = impl.getConnectionProvider();
			// if (connectionProvider instanceof C3P0ConnectionProvider) {
			// ((C3P0ConnectionProvider) connectionProvider).stop();
			// }
			// }
			HibernateUtil._sessionFactory.close();
			HibernateUtil._sessionFactory = null;
			HibernateUtil._configuration = null;
			System.out.println("Close session factory success");
		}
		catch (HibernateException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Rollback transaction of the input session.
	 * 
	 * @param hibernateSession the hibernate session
	 */
	public static void rollbackTransaction(Session hibernateSession) {
		hibernateSession.getTransaction().rollback();
	}

	/**
	 * Sets the config path.
	 *
	 * @param configPath the new config path
	 */
	public static void setConfigPath(String configPath) {
		HibernateUtil._configFile = configPath;
	}

}
