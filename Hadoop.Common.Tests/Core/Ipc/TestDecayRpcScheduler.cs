using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class TestDecayRpcScheduler
	{
		private Schedulable MockCall(string id)
		{
			Schedulable mockCall = Org.Mockito.Mockito.Mock<Schedulable>();
			UserGroupInformation ugi = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(ugi.GetUserName()).ThenReturn(id);
			Org.Mockito.Mockito.When(mockCall.GetUserGroupInformation()).ThenReturn(ugi);
			return mockCall;
		}

		private DecayRpcScheduler scheduler;

		public virtual void TestNegativeScheduler()
		{
			scheduler = new DecayRpcScheduler(-1, string.Empty, new Configuration());
		}

		public virtual void TestZeroScheduler()
		{
			scheduler = new DecayRpcScheduler(0, string.Empty, new Configuration());
		}

		[NUnit.Framework.Test]
		public virtual void TestParsePeriod()
		{
			// By default
			scheduler = new DecayRpcScheduler(1, string.Empty, new Configuration());
			NUnit.Framework.Assert.AreEqual(DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodDefault
				, scheduler.GetDecayPeriodMillis());
			// Custom
			Configuration conf = new Configuration();
			conf.SetLong("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodKey, 1058);
			scheduler = new DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(1058L, scheduler.GetDecayPeriodMillis());
		}

		[NUnit.Framework.Test]
		public virtual void TestParseFactor()
		{
			// Default
			scheduler = new DecayRpcScheduler(1, string.Empty, new Configuration());
			NUnit.Framework.Assert.AreEqual(DecayRpcScheduler.IpcCallqueueDecayschedulerFactorDefault
				, scheduler.GetDecayFactor(), 0.00001);
			// Custom
			Configuration conf = new Configuration();
			conf.Set("prefix." + DecayRpcScheduler.IpcCallqueueDecayschedulerFactorKey, "0.125"
				);
			scheduler = new DecayRpcScheduler(1, "prefix", conf);
			NUnit.Framework.Assert.AreEqual(0.125, scheduler.GetDecayFactor(), 0.00001);
		}

		public virtual void AssertEqualDecimalArrays(double[] a, double[] b)
		{
			NUnit.Framework.Assert.AreEqual(a.Length, b.Length);
			for (int i = 0; i < a.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(a[i], b[i], 0.00001);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestParseThresholds()
		{
			// Defaults vary by number of queues
			Configuration conf = new Configuration();
			scheduler = new DecayRpcScheduler(1, string.Empty, conf);
			AssertEqualDecimalArrays(new double[] {  }, scheduler.GetThresholds());
			scheduler = new DecayRpcScheduler(2, string.Empty, conf);
			AssertEqualDecimalArrays(new double[] { 0.5 }, scheduler.GetThresholds());
			scheduler = new DecayRpcScheduler(3, string.Empty, conf);
			AssertEqualDecimalArrays(new double[] { 0.25, 0.5 }, scheduler.GetThresholds());
			scheduler = new DecayRpcScheduler(4, string.Empty, conf);
			AssertEqualDecimalArrays(new double[] { 0.125, 0.25, 0.5 }, scheduler.GetThresholds
				());
			// Custom
			conf = new Configuration();
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerThresholdsKey, "1, 10, 20, 50, 85"
				);
			scheduler = new DecayRpcScheduler(6, "ns", conf);
			AssertEqualDecimalArrays(new double[] { 0.01, 0.1, 0.2, 0.5, 0.85 }, scheduler.GetThresholds
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestAccumulate()
		{
			Configuration conf = new Configuration();
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodKey, "99999999"
				);
			// Never flush
			scheduler = new DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetCallCountSnapshot().Count);
			// empty first
			scheduler.GetPriorityLevel(MockCall("A"));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetCallCountSnapshot()["A"]);
			scheduler.GetPriorityLevel(MockCall("A"));
			scheduler.GetPriorityLevel(MockCall("B"));
			scheduler.GetPriorityLevel(MockCall("A"));
			NUnit.Framework.Assert.AreEqual(3, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetCallCountSnapshot()["B"]);
		}

		[NUnit.Framework.Test]
		public virtual void TestDecay()
		{
			Configuration conf = new Configuration();
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodKey, "999999999"
				);
			// Never
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerFactorKey, "0.5");
			scheduler = new DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetTotalCallSnapshot());
			for (int i = 0; i < 4; i++)
			{
				scheduler.GetPriorityLevel(MockCall("A"));
			}
			for (int i_1 = 0; i_1 < 8; i_1++)
			{
				scheduler.GetPriorityLevel(MockCall("B"));
			}
			NUnit.Framework.Assert.AreEqual(12, scheduler.GetTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(8, scheduler.GetCallCountSnapshot()["B"]);
			scheduler.ForceDecay();
			NUnit.Framework.Assert.AreEqual(6, scheduler.GetTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetCallCountSnapshot()["B"]);
			scheduler.ForceDecay();
			NUnit.Framework.Assert.AreEqual(3, scheduler.GetTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetCallCountSnapshot()["B"]);
			scheduler.ForceDecay();
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(null, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetCallCountSnapshot()["B"]);
			scheduler.ForceDecay();
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(null, scheduler.GetCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(null, scheduler.GetCallCountSnapshot()["B"]);
		}

		[NUnit.Framework.Test]
		public virtual void TestPriority()
		{
			Configuration conf = new Configuration();
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodKey, "99999999"
				);
			// Never flush
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerThresholdsKey, "25, 50, 75"
				);
			scheduler = new DecayRpcScheduler(4, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetPriorityLevel(MockCall("A")));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetPriorityLevel(MockCall("A")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetPriorityLevel(MockCall("B")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetPriorityLevel(MockCall("B")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetPriorityLevel(MockCall("C")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetPriorityLevel(MockCall("C")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetPriorityLevel(MockCall("A")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetPriorityLevel(MockCall("A")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetPriorityLevel(MockCall("A")));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetPriorityLevel(MockCall("A")));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPeriodic()
		{
			Configuration conf = new Configuration();
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerPeriodKey, "10");
			conf.Set("ns." + DecayRpcScheduler.IpcCallqueueDecayschedulerFactorKey, "0.5");
			scheduler = new DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(10, scheduler.GetDecayPeriodMillis());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetTotalCallSnapshot());
			for (int i = 0; i < 64; i++)
			{
				scheduler.GetPriorityLevel(MockCall("A"));
			}
			// It should eventually decay to zero
			while (scheduler.GetTotalCallSnapshot() > 0)
			{
				Sharpen.Thread.Sleep(10);
			}
		}
	}
}
