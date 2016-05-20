using Sharpen;

namespace org.apache.hadoop.conf
{
	public class TestReconfiguration
	{
		private org.apache.hadoop.conf.Configuration conf1;

		private org.apache.hadoop.conf.Configuration conf2;

		private const string PROP1 = "test.prop.one";

		private const string PROP2 = "test.prop.two";

		private const string PROP3 = "test.prop.three";

		private const string PROP4 = "test.prop.four";

		private const string PROP5 = "test.prop.five";

		private const string VAL1 = "val1";

		private const string VAL2 = "val2";

		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			conf1 = new org.apache.hadoop.conf.Configuration();
			conf2 = new org.apache.hadoop.conf.Configuration();
			// set some test properties
			conf1.set(PROP1, VAL1);
			conf1.set(PROP2, VAL1);
			conf1.set(PROP3, VAL1);
			conf2.set(PROP1, VAL1);
			// same as conf1
			conf2.set(PROP2, VAL2);
			// different value as conf1
			// PROP3 not set in conf2
			conf2.set(PROP4, VAL1);
		}

		// not set in conf1
		/// <summary>Test ReconfigurationUtil.getChangedProperties.</summary>
		[NUnit.Framework.Test]
		public virtual void testGetChangedProperties()
		{
			System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				> changes = org.apache.hadoop.conf.ReconfigurationUtil.getChangedProperties(conf2
				, conf1);
			NUnit.Framework.Assert.IsTrue("expected 3 changed properties but got " + changes.
				Count, changes.Count == 3);
			bool changeFound = false;
			bool unsetFound = false;
			bool setFound = false;
			foreach (org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange c in changes)
			{
				if (c.prop.Equals(PROP2) && c.oldVal != null && c.oldVal.Equals(VAL1) && c.newVal
					 != null && c.newVal.Equals(VAL2))
				{
					changeFound = true;
				}
				else
				{
					if (c.prop.Equals(PROP3) && c.oldVal != null && c.oldVal.Equals(VAL1) && c.newVal
						 == null)
					{
						unsetFound = true;
					}
					else
					{
						if (c.prop.Equals(PROP4) && c.oldVal == null && c.newVal != null && c.newVal.Equals
							(VAL1))
						{
							setFound = true;
						}
					}
				}
			}
			NUnit.Framework.Assert.IsTrue("not all changes have been applied", changeFound &&
				 unsetFound && setFound);
		}

		/// <summary>a simple reconfigurable class</summary>
		public class ReconfigurableDummy : org.apache.hadoop.conf.ReconfigurableBase, java.lang.Runnable
		{
			public volatile bool running = true;

			public ReconfigurableDummy(org.apache.hadoop.conf.Configuration conf)
				: base(conf)
			{
			}

			public override System.Collections.Generic.ICollection<string> getReconfigurableProperties
				()
			{
				return java.util.Arrays.asList(PROP1, PROP2, PROP4);
			}

			/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
			protected internal override void reconfigurePropertyImpl(string property, string 
				newVal)
			{
				lock (this)
				{
				}
			}

			// do nothing
			/// <summary>Run until PROP1 is no longer VAL1.</summary>
			public virtual void run()
			{
				while (running && getConf().get(PROP1).Equals(VAL1))
				{
					try
					{
						java.lang.Thread.sleep(1);
					}
					catch (System.Exception)
					{
					}
				}
			}
			// do nothing
		}

		/// <summary>Test reconfiguring a Reconfigurable.</summary>
		[NUnit.Framework.Test]
		public virtual void testReconfigure()
		{
			org.apache.hadoop.conf.TestReconfiguration.ReconfigurableDummy dummy = new org.apache.hadoop.conf.TestReconfiguration.ReconfigurableDummy
				(conf1);
			NUnit.Framework.Assert.IsTrue(PROP1 + " set to wrong value ", dummy.getConf().get
				(PROP1).Equals(VAL1));
			NUnit.Framework.Assert.IsTrue(PROP2 + " set to wrong value ", dummy.getConf().get
				(PROP2).Equals(VAL1));
			NUnit.Framework.Assert.IsTrue(PROP3 + " set to wrong value ", dummy.getConf().get
				(PROP3).Equals(VAL1));
			NUnit.Framework.Assert.IsTrue(PROP4 + " set to wrong value ", dummy.getConf().get
				(PROP4) == null);
			NUnit.Framework.Assert.IsTrue(PROP5 + " set to wrong value ", dummy.getConf().get
				(PROP5) == null);
			NUnit.Framework.Assert.IsTrue(PROP1 + " should be reconfigurable ", dummy.isPropertyReconfigurable
				(PROP1));
			NUnit.Framework.Assert.IsTrue(PROP2 + " should be reconfigurable ", dummy.isPropertyReconfigurable
				(PROP2));
			NUnit.Framework.Assert.IsFalse(PROP3 + " should not be reconfigurable ", dummy.isPropertyReconfigurable
				(PROP3));
			NUnit.Framework.Assert.IsTrue(PROP4 + " should be reconfigurable ", dummy.isPropertyReconfigurable
				(PROP4));
			NUnit.Framework.Assert.IsFalse(PROP5 + " should not be reconfigurable ", dummy.isPropertyReconfigurable
				(PROP5));
			{
				// change something to the same value as before
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP1, VAL1);
					NUnit.Framework.Assert.IsTrue(PROP1 + " set to wrong value ", dummy.getConf().get
						(PROP1).Equals(VAL1));
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsFalse("received unexpected exception", exceptionCaught);
			}
			{
				// change something to null
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP1, null);
					NUnit.Framework.Assert.IsTrue(PROP1 + "set to wrong value ", dummy.getConf().get(
						PROP1) == null);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsFalse("received unexpected exception", exceptionCaught);
			}
			{
				// change something to a different value than before
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP1, VAL2);
					NUnit.Framework.Assert.IsTrue(PROP1 + "set to wrong value ", dummy.getConf().get(
						PROP1).Equals(VAL2));
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsFalse("received unexpected exception", exceptionCaught);
			}
			{
				// set unset property to null
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP4, null);
					NUnit.Framework.Assert.IsTrue(PROP4 + "set to wrong value ", dummy.getConf().get(
						PROP4) == null);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsFalse("received unexpected exception", exceptionCaught);
			}
			{
				// set unset property
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP4, VAL1);
					NUnit.Framework.Assert.IsTrue(PROP4 + "set to wrong value ", dummy.getConf().get(
						PROP4).Equals(VAL1));
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsFalse("received unexpected exception", exceptionCaught);
			}
			{
				// try to set unset property to null (not reconfigurable)
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP5, null);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsTrue("did not receive expected exception", exceptionCaught
					);
			}
			{
				// try to set unset property to value (not reconfigurable)
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP5, VAL1);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsTrue("did not receive expected exception", exceptionCaught
					);
			}
			{
				// try to change property to value (not reconfigurable)
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP3, VAL2);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsTrue("did not receive expected exception", exceptionCaught
					);
			}
			{
				// try to change property to null (not reconfigurable)
				bool exceptionCaught = false;
				try
				{
					dummy.reconfigureProperty(PROP3, null);
				}
				catch (org.apache.hadoop.conf.ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsTrue("did not receive expected exception", exceptionCaught
					);
			}
		}

		/// <summary>Test whether configuration changes are visible in another thread.</summary>
		/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testThread()
		{
			org.apache.hadoop.conf.TestReconfiguration.ReconfigurableDummy dummy = new org.apache.hadoop.conf.TestReconfiguration.ReconfigurableDummy
				(conf1);
			NUnit.Framework.Assert.IsTrue(dummy.getConf().get(PROP1).Equals(VAL1));
			java.lang.Thread dummyThread = new java.lang.Thread(dummy);
			dummyThread.start();
			try
			{
				java.lang.Thread.sleep(500);
			}
			catch (System.Exception)
			{
			}
			// do nothing
			dummy.reconfigureProperty(PROP1, VAL2);
			long endWait = org.apache.hadoop.util.Time.now() + 2000;
			while (dummyThread.isAlive() && org.apache.hadoop.util.Time.now() < endWait)
			{
				try
				{
					java.lang.Thread.sleep(50);
				}
				catch (System.Exception)
				{
				}
			}
			// do nothing
			NUnit.Framework.Assert.IsFalse("dummy thread should not be alive", dummyThread.isAlive
				());
			dummy.running = false;
			try
			{
				dummyThread.join();
			}
			catch (System.Exception)
			{
			}
			// do nothing
			NUnit.Framework.Assert.IsTrue(PROP1 + " is set to wrong value", dummy.getConf().get
				(PROP1).Equals(VAL2));
		}

		private class AsyncReconfigurableDummy : org.apache.hadoop.conf.ReconfigurableBase
		{
			internal AsyncReconfigurableDummy(org.apache.hadoop.conf.Configuration conf)
				: base(conf)
			{
			}

			internal readonly java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(1);

			public override System.Collections.Generic.ICollection<string> getReconfigurableProperties
				()
			{
				return java.util.Arrays.asList(PROP1, PROP2, PROP4);
			}

			/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
			protected internal override void reconfigurePropertyImpl(string property, string 
				newVal)
			{
				lock (this)
				{
					try
					{
						latch.await();
					}
					catch (System.Exception)
					{
					}
				}
			}
			// Ignore
		}

		/// <exception cref="System.Exception"/>
		private static void waitAsyncReconfigureTaskFinish(org.apache.hadoop.conf.ReconfigurableBase
			 rb)
		{
			org.apache.hadoop.conf.ReconfigurationTaskStatus status = null;
			int count = 20;
			while (count > 0)
			{
				status = rb.getReconfigurationTaskStatus();
				if (status.stopped())
				{
					break;
				}
				count--;
				java.lang.Thread.sleep(500);
			}
			System.Diagnostics.Debug.Assert((status.stopped()));
		}

		/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAsyncReconfigure()
		{
			org.apache.hadoop.conf.TestReconfiguration.AsyncReconfigurableDummy dummy = org.mockito.Mockito.spy
				(new org.apache.hadoop.conf.TestReconfiguration.AsyncReconfigurableDummy(conf1));
			System.Collections.Generic.IList<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				> changes = com.google.common.collect.Lists.newArrayList();
			changes.add(new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange("name1"
				, "new1", "old1"));
			changes.add(new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange("name2"
				, "new2", "old2"));
			changes.add(new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange("name3"
				, "new3", "old3"));
			org.mockito.Mockito.doReturn(changes).when(dummy).getChangedProperties(org.mockito.Matchers.any
				<org.apache.hadoop.conf.Configuration>(), org.mockito.Matchers.any<org.apache.hadoop.conf.Configuration
				>());
			org.mockito.Mockito.doReturn(true).when(dummy).isPropertyReconfigurable(org.mockito.Matchers.eq
				("name1"));
			org.mockito.Mockito.doReturn(false).when(dummy).isPropertyReconfigurable(org.mockito.Matchers.eq
				("name2"));
			org.mockito.Mockito.doReturn(true).when(dummy).isPropertyReconfigurable(org.mockito.Matchers.eq
				("name3"));
			org.mockito.Mockito.doNothing().when(dummy).reconfigurePropertyImpl(org.mockito.Matchers.eq
				("name1"), org.mockito.Matchers.anyString());
			org.mockito.Mockito.doNothing().when(dummy).reconfigurePropertyImpl(org.mockito.Matchers.eq
				("name2"), org.mockito.Matchers.anyString());
			org.mockito.Mockito.doThrow(new org.apache.hadoop.conf.ReconfigurationException("NAME3"
				, "NEW3", "OLD3", new System.IO.IOException("io exception"))).when(dummy).reconfigurePropertyImpl
				(org.mockito.Matchers.eq("name3"), org.mockito.Matchers.anyString());
			dummy.startReconfigurationTask();
			waitAsyncReconfigureTaskFinish(dummy);
			org.apache.hadoop.conf.ReconfigurationTaskStatus status = dummy.getReconfigurationTaskStatus
				();
			NUnit.Framework.Assert.AreEqual(3, status.getStatus().Count);
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				, com.google.common.@base.Optional<string>> result in status.getStatus())
			{
				org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange change = result.Key;
				if (change.prop.Equals("name1"))
				{
					NUnit.Framework.Assert.IsFalse(result.Value.isPresent());
				}
				else
				{
					if (change.prop.Equals("name2"))
					{
						org.hamcrest.MatcherAssert.assertThat(result.Value.get(), org.hamcrest.CoreMatchers.containsString
							("Property name2 is not reconfigurable"));
					}
					else
					{
						if (change.prop.Equals("name3"))
						{
							org.hamcrest.MatcherAssert.assertThat(result.Value.get(), org.hamcrest.CoreMatchers.containsString
								("io exception"));
						}
						else
						{
							NUnit.Framework.Assert.Fail("Unknown property: " + change.prop);
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStartReconfigurationFailureDueToExistingRunningTask()
		{
			org.apache.hadoop.conf.TestReconfiguration.AsyncReconfigurableDummy dummy = org.mockito.Mockito.spy
				(new org.apache.hadoop.conf.TestReconfiguration.AsyncReconfigurableDummy(conf1));
			System.Collections.Generic.IList<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				> changes = com.google.common.collect.Lists.newArrayList(new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				(PROP1, "new1", "old1"));
			org.mockito.Mockito.doReturn(changes).when(dummy).getChangedProperties(org.mockito.Matchers.any
				<org.apache.hadoop.conf.Configuration>(), org.mockito.Matchers.any<org.apache.hadoop.conf.Configuration
				>());
			org.apache.hadoop.conf.ReconfigurationTaskStatus status = dummy.getReconfigurationTaskStatus
				();
			NUnit.Framework.Assert.IsFalse(status.hasTask());
			dummy.startReconfigurationTask();
			status = dummy.getReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.hasTask());
			NUnit.Framework.Assert.IsFalse(status.stopped());
			// An active reconfiguration task is running.
			try
			{
				dummy.startReconfigurationTask();
				NUnit.Framework.Assert.Fail("Expect to throw IOException.");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Another reconfiguration task is running"
					, e);
			}
			status = dummy.getReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.hasTask());
			NUnit.Framework.Assert.IsFalse(status.stopped());
			dummy.latch.countDown();
			waitAsyncReconfigureTaskFinish(dummy);
			status = dummy.getReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.hasTask());
			NUnit.Framework.Assert.IsTrue(status.stopped());
			// The first task has finished.
			dummy.startReconfigurationTask();
			waitAsyncReconfigureTaskFinish(dummy);
			org.apache.hadoop.conf.ReconfigurationTaskStatus status2 = dummy.getReconfigurationTaskStatus
				();
			NUnit.Framework.Assert.IsTrue(status2.getStartTime() >= status.getEndTime());
			dummy.shutdownReconfigurationTask();
			try
			{
				dummy.startReconfigurationTask();
				NUnit.Framework.Assert.Fail("Expect to throw IOException");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("The server is stopped"
					, e);
			}
		}
	}
}
