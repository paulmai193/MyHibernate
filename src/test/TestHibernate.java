import logia.hibernate.util.HibernateUtil;

import org.hibernate.Session;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHibernate {

	Session session;

	@Before
	public void setUp() throws Exception {
		HibernateUtil.setConfigPath("hibernate-sample.cfg.xml");

	}

	@After
	public void tearDown() throws Exception {
		HibernateUtil.closeSession(this.session);
		HibernateUtil.releaseFactory();
	}

	@Test
	public void test() {
		this.session = HibernateUtil.getSession();
		Assert.assertTrue("Session is opened", this.session.isOpen());
		Assert.assertTrue("Session is connected", this.session.isConnected());
	}

}
