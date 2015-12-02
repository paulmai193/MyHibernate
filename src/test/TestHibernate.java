import logia.hibernate.util.HibernateUtil;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHibernate {

	@Before
	public void setUp() throws Exception {
		HibernateUtil.setConfigPath("hibernate-sample.cfg.xml");

	}

	@After
	public void tearDown() throws Exception {
		HibernateUtil.releaseFactory();
	}

	@Test
	public void test() throws NumberFormatException, HibernateException, Exception {
		Session session = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session1 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session2 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session3 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session4 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session5 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

		Session session6 = HibernateUtil.beginTransaction();

		System.out.println("Opened: " + HibernateUtil.getStatistics().getSessionOpenCount());
		System.out.println("Close: " + HibernateUtil.getStatistics().getSessionCloseCount());
		System.out.println("Opening: " + HibernateUtil.getStatistics().getConnectCount());

	}

}
