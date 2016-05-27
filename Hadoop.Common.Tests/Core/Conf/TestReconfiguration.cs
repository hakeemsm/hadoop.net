using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class TestReconfiguration
	{
		private Configuration conf1;

		private Configuration conf2;

		private const string Prop1 = "test.prop.one";

		private const string Prop2 = "test.prop.two";

		private const string Prop3 = "test.prop.three";

		private const string Prop4 = "test.prop.four";

		private const string Prop5 = "test.prop.five";

		private const string Val1 = "val1";

		private const string Val2 = "val2";

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf1 = new Configuration();
			conf2 = new Configuration();
			// set some test properties
			conf1.Set(Prop1, Val1);
			conf1.Set(Prop2, Val1);
			conf1.Set(Prop3, Val1);
			conf2.Set(Prop1, Val1);
			// same as conf1
			conf2.Set(Prop2, Val2);
			// different value as conf1
			// PROP3 not set in conf2
			conf2.Set(Prop4, Val1);
		}

		// not set in conf1
		/// <summary>Test ReconfigurationUtil.getChangedProperties.</summary>
		[NUnit.Framework.Test]
		public virtual void TestGetChangedProperties()
		{
			ICollection<ReconfigurationUtil.PropertyChange> changes = ReconfigurationUtil.GetChangedProperties
				(conf2, conf1);
			NUnit.Framework.Assert.IsTrue("expected 3 changed properties but got " + changes.
				Count, changes.Count == 3);
			bool changeFound = false;
			bool unsetFound = false;
			bool setFound = false;
			foreach (ReconfigurationUtil.PropertyChange c in changes)
			{
				if (c.prop.Equals(Prop2) && c.oldVal != null && c.oldVal.Equals(Val1) && c.newVal
					 != null && c.newVal.Equals(Val2))
				{
					changeFound = true;
				}
				else
				{
					if (c.prop.Equals(Prop3) && c.oldVal != null && c.oldVal.Equals(Val1) && c.newVal
						 == null)
					{
						unsetFound = true;
					}
					else
					{
						if (c.prop.Equals(Prop4) && c.oldVal == null && c.newVal != null && c.newVal.Equals
							(Val1))
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
		public class ReconfigurableDummy : ReconfigurableBase, Runnable
		{
			public volatile bool running = true;

			public ReconfigurableDummy(Configuration conf)
				: base(conf)
			{
			}

			public override ICollection<string> GetReconfigurableProperties()
			{
				return Arrays.AsList(Prop1, Prop2, Prop4);
			}

			/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
			protected internal override void ReconfigurePropertyImpl(string property, string 
				newVal)
			{
				lock (this)
				{
				}
			}

			// do nothing
			/// <summary>Run until PROP1 is no longer VAL1.</summary>
			public virtual void Run()
			{
				while (running && GetConf().Get(Prop1).Equals(Val1))
				{
					try
					{
						Sharpen.Thread.Sleep(1);
					}
					catch (Exception)
					{
					}
				}
			}
			// do nothing
		}

		/// <summary>Test reconfiguring a Reconfigurable.</summary>
		[NUnit.Framework.Test]
		public virtual void TestReconfigure()
		{
			TestReconfiguration.ReconfigurableDummy dummy = new TestReconfiguration.ReconfigurableDummy
				(conf1);
			NUnit.Framework.Assert.IsTrue(Prop1 + " set to wrong value ", dummy.GetConf().Get
				(Prop1).Equals(Val1));
			NUnit.Framework.Assert.IsTrue(Prop2 + " set to wrong value ", dummy.GetConf().Get
				(Prop2).Equals(Val1));
			NUnit.Framework.Assert.IsTrue(Prop3 + " set to wrong value ", dummy.GetConf().Get
				(Prop3).Equals(Val1));
			NUnit.Framework.Assert.IsTrue(Prop4 + " set to wrong value ", dummy.GetConf().Get
				(Prop4) == null);
			NUnit.Framework.Assert.IsTrue(Prop5 + " set to wrong value ", dummy.GetConf().Get
				(Prop5) == null);
			NUnit.Framework.Assert.IsTrue(Prop1 + " should be reconfigurable ", dummy.IsPropertyReconfigurable
				(Prop1));
			NUnit.Framework.Assert.IsTrue(Prop2 + " should be reconfigurable ", dummy.IsPropertyReconfigurable
				(Prop2));
			NUnit.Framework.Assert.IsFalse(Prop3 + " should not be reconfigurable ", dummy.IsPropertyReconfigurable
				(Prop3));
			NUnit.Framework.Assert.IsTrue(Prop4 + " should be reconfigurable ", dummy.IsPropertyReconfigurable
				(Prop4));
			NUnit.Framework.Assert.IsFalse(Prop5 + " should not be reconfigurable ", dummy.IsPropertyReconfigurable
				(Prop5));
			{
				// change something to the same value as before
				bool exceptionCaught = false;
				try
				{
					dummy.ReconfigureProperty(Prop1, Val1);
					NUnit.Framework.Assert.IsTrue(Prop1 + " set to wrong value ", dummy.GetConf().Get
						(Prop1).Equals(Val1));
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop1, null);
					NUnit.Framework.Assert.IsTrue(Prop1 + "set to wrong value ", dummy.GetConf().Get(
						Prop1) == null);
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop1, Val2);
					NUnit.Framework.Assert.IsTrue(Prop1 + "set to wrong value ", dummy.GetConf().Get(
						Prop1).Equals(Val2));
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop4, null);
					NUnit.Framework.Assert.IsTrue(Prop4 + "set to wrong value ", dummy.GetConf().Get(
						Prop4) == null);
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop4, Val1);
					NUnit.Framework.Assert.IsTrue(Prop4 + "set to wrong value ", dummy.GetConf().Get(
						Prop4).Equals(Val1));
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop5, null);
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop5, Val1);
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop3, Val2);
				}
				catch (ReconfigurationException)
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
					dummy.ReconfigureProperty(Prop3, null);
				}
				catch (ReconfigurationException)
				{
					exceptionCaught = true;
				}
				NUnit.Framework.Assert.IsTrue("did not receive expected exception", exceptionCaught
					);
			}
		}

		/// <summary>Test whether configuration changes are visible in another thread.</summary>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void TestThread()
		{
			TestReconfiguration.ReconfigurableDummy dummy = new TestReconfiguration.ReconfigurableDummy
				(conf1);
			NUnit.Framework.Assert.IsTrue(dummy.GetConf().Get(Prop1).Equals(Val1));
			Sharpen.Thread dummyThread = new Sharpen.Thread(dummy);
			dummyThread.Start();
			try
			{
				Sharpen.Thread.Sleep(500);
			}
			catch (Exception)
			{
			}
			// do nothing
			dummy.ReconfigureProperty(Prop1, Val2);
			long endWait = Time.Now() + 2000;
			while (dummyThread.IsAlive() && Time.Now() < endWait)
			{
				try
				{
					Sharpen.Thread.Sleep(50);
				}
				catch (Exception)
				{
				}
			}
			// do nothing
			NUnit.Framework.Assert.IsFalse("dummy thread should not be alive", dummyThread.IsAlive
				());
			dummy.running = false;
			try
			{
				dummyThread.Join();
			}
			catch (Exception)
			{
			}
			// do nothing
			NUnit.Framework.Assert.IsTrue(Prop1 + " is set to wrong value", dummy.GetConf().Get
				(Prop1).Equals(Val2));
		}

		private class AsyncReconfigurableDummy : ReconfigurableBase
		{
			internal AsyncReconfigurableDummy(Configuration conf)
				: base(conf)
			{
			}

			internal readonly CountDownLatch latch = new CountDownLatch(1);

			public override ICollection<string> GetReconfigurableProperties()
			{
				return Arrays.AsList(Prop1, Prop2, Prop4);
			}

			/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
			protected internal override void ReconfigurePropertyImpl(string property, string 
				newVal)
			{
				lock (this)
				{
					try
					{
						latch.Await();
					}
					catch (Exception)
					{
					}
				}
			}
			// Ignore
		}

		/// <exception cref="System.Exception"/>
		private static void WaitAsyncReconfigureTaskFinish(ReconfigurableBase rb)
		{
			ReconfigurationTaskStatus status = null;
			int count = 20;
			while (count > 0)
			{
				status = rb.GetReconfigurationTaskStatus();
				if (status.Stopped())
				{
					break;
				}
				count--;
				Sharpen.Thread.Sleep(500);
			}
			System.Diagnostics.Debug.Assert((status.Stopped()));
		}

		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAsyncReconfigure()
		{
			TestReconfiguration.AsyncReconfigurableDummy dummy = Org.Mockito.Mockito.Spy(new 
				TestReconfiguration.AsyncReconfigurableDummy(conf1));
			IList<ReconfigurationUtil.PropertyChange> changes = Lists.NewArrayList();
			changes.AddItem(new ReconfigurationUtil.PropertyChange("name1", "new1", "old1"));
			changes.AddItem(new ReconfigurationUtil.PropertyChange("name2", "new2", "old2"));
			changes.AddItem(new ReconfigurationUtil.PropertyChange("name3", "new3", "old3"));
			Org.Mockito.Mockito.DoReturn(changes).When(dummy).GetChangedProperties(Matchers.Any
				<Configuration>(), Matchers.Any<Configuration>());
			Org.Mockito.Mockito.DoReturn(true).When(dummy).IsPropertyReconfigurable(Matchers.Eq
				("name1"));
			Org.Mockito.Mockito.DoReturn(false).When(dummy).IsPropertyReconfigurable(Matchers.Eq
				("name2"));
			Org.Mockito.Mockito.DoReturn(true).When(dummy).IsPropertyReconfigurable(Matchers.Eq
				("name3"));
			Org.Mockito.Mockito.DoNothing().When(dummy).ReconfigurePropertyImpl(Matchers.Eq("name1"
				), Matchers.AnyString());
			Org.Mockito.Mockito.DoNothing().When(dummy).ReconfigurePropertyImpl(Matchers.Eq("name2"
				), Matchers.AnyString());
			Org.Mockito.Mockito.DoThrow(new ReconfigurationException("NAME3", "NEW3", "OLD3", 
				new IOException("io exception"))).When(dummy).ReconfigurePropertyImpl(Matchers.Eq
				("name3"), Matchers.AnyString());
			dummy.StartReconfigurationTask();
			WaitAsyncReconfigureTaskFinish(dummy);
			ReconfigurationTaskStatus status = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.AreEqual(3, status.GetStatus().Count);
			foreach (KeyValuePair<ReconfigurationUtil.PropertyChange, Optional<string>> result
				 in status.GetStatus())
			{
				ReconfigurationUtil.PropertyChange change = result.Key;
				if (change.prop.Equals("name1"))
				{
					NUnit.Framework.Assert.IsFalse(result.Value.IsPresent());
				}
				else
				{
					if (change.prop.Equals("name2"))
					{
						MatcherAssert.AssertThat(result.Value.Get(), CoreMatchers.ContainsString("Property name2 is not reconfigurable"
							));
					}
					else
					{
						if (change.prop.Equals("name3"))
						{
							MatcherAssert.AssertThat(result.Value.Get(), CoreMatchers.ContainsString("io exception"
								));
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
		public virtual void TestStartReconfigurationFailureDueToExistingRunningTask()
		{
			TestReconfiguration.AsyncReconfigurableDummy dummy = Org.Mockito.Mockito.Spy(new 
				TestReconfiguration.AsyncReconfigurableDummy(conf1));
			IList<ReconfigurationUtil.PropertyChange> changes = Lists.NewArrayList(new ReconfigurationUtil.PropertyChange
				(Prop1, "new1", "old1"));
			Org.Mockito.Mockito.DoReturn(changes).When(dummy).GetChangedProperties(Matchers.Any
				<Configuration>(), Matchers.Any<Configuration>());
			ReconfigurationTaskStatus status = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsFalse(status.HasTask());
			dummy.StartReconfigurationTask();
			status = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.HasTask());
			NUnit.Framework.Assert.IsFalse(status.Stopped());
			// An active reconfiguration task is running.
			try
			{
				dummy.StartReconfigurationTask();
				NUnit.Framework.Assert.Fail("Expect to throw IOException.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Another reconfiguration task is running"
					, e);
			}
			status = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.HasTask());
			NUnit.Framework.Assert.IsFalse(status.Stopped());
			dummy.latch.CountDown();
			WaitAsyncReconfigureTaskFinish(dummy);
			status = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status.HasTask());
			NUnit.Framework.Assert.IsTrue(status.Stopped());
			// The first task has finished.
			dummy.StartReconfigurationTask();
			WaitAsyncReconfigureTaskFinish(dummy);
			ReconfigurationTaskStatus status2 = dummy.GetReconfigurationTaskStatus();
			NUnit.Framework.Assert.IsTrue(status2.GetStartTime() >= status.GetEndTime());
			dummy.ShutdownReconfigurationTask();
			try
			{
				dummy.StartReconfigurationTask();
				NUnit.Framework.Assert.Fail("Expect to throw IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("The server is stopped", e);
			}
		}
	}
}
