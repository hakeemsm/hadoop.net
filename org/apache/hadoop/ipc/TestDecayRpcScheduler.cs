using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestDecayRpcScheduler
	{
		private org.apache.hadoop.ipc.Schedulable mockCall(string id)
		{
			org.apache.hadoop.ipc.Schedulable mockCall = org.mockito.Mockito.mock<org.apache.hadoop.ipc.Schedulable
				>();
			org.apache.hadoop.security.UserGroupInformation ugi = org.mockito.Mockito.mock<org.apache.hadoop.security.UserGroupInformation
				>();
			org.mockito.Mockito.when(ugi.getUserName()).thenReturn(id);
			org.mockito.Mockito.when(mockCall.getUserGroupInformation()).thenReturn(ugi);
			return mockCall;
		}

		private org.apache.hadoop.ipc.DecayRpcScheduler scheduler;

		public virtual void testNegativeScheduler()
		{
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(-1, string.Empty, new org.apache.hadoop.conf.Configuration
				());
		}

		public virtual void testZeroScheduler()
		{
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(0, string.Empty, new org.apache.hadoop.conf.Configuration
				());
		}

		[NUnit.Framework.Test]
		public virtual void testParsePeriod()
		{
			// By default
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, string.Empty, new org.apache.hadoop.conf.Configuration
				());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT
				, scheduler.getDecayPeriodMillis());
			// Custom
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setLong("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY
				, 1058);
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(1058L, scheduler.getDecayPeriodMillis());
		}

		[NUnit.Framework.Test]
		public virtual void testParseFactor()
		{
			// Default
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, string.Empty, new org.apache.hadoop.conf.Configuration
				());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT
				, scheduler.getDecayFactor(), 0.00001);
			// Custom
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("prefix." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY
				, "0.125");
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, "prefix", conf);
			NUnit.Framework.Assert.AreEqual(0.125, scheduler.getDecayFactor(), 0.00001);
		}

		public virtual void assertEqualDecimalArrays(double[] a, double[] b)
		{
			NUnit.Framework.Assert.AreEqual(a.Length, b.Length);
			for (int i = 0; i < a.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(a[i], b[i], 0.00001);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testParseThresholds()
		{
			// Defaults vary by number of queues
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, string.Empty, conf);
			assertEqualDecimalArrays(new double[] {  }, scheduler.getThresholds());
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(2, string.Empty, conf);
			assertEqualDecimalArrays(new double[] { 0.5 }, scheduler.getThresholds());
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(3, string.Empty, conf);
			assertEqualDecimalArrays(new double[] { 0.25, 0.5 }, scheduler.getThresholds());
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(4, string.Empty, conf);
			assertEqualDecimalArrays(new double[] { 0.125, 0.25, 0.5 }, scheduler.getThresholds
				());
			// Custom
			conf = new org.apache.hadoop.conf.Configuration();
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY
				, "1, 10, 20, 50, 85");
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(6, "ns", conf);
			assertEqualDecimalArrays(new double[] { 0.01, 0.1, 0.2, 0.5, 0.85 }, scheduler.getThresholds
				());
		}

		[NUnit.Framework.Test]
		public virtual void testAccumulate()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY
				, "99999999");
			// Never flush
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.getCallCountSnapshot().Count);
			// empty first
			scheduler.getPriorityLevel(mockCall("A"));
			NUnit.Framework.Assert.AreEqual(1, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.getCallCountSnapshot()["A"]);
			scheduler.getPriorityLevel(mockCall("A"));
			scheduler.getPriorityLevel(mockCall("B"));
			scheduler.getPriorityLevel(mockCall("A"));
			NUnit.Framework.Assert.AreEqual(3, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.getCallCountSnapshot()["B"]);
		}

		[NUnit.Framework.Test]
		public virtual void testDecay()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY
				, "999999999");
			// Never
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY
				, "0.5");
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.getTotalCallSnapshot());
			for (int i = 0; i < 4; i++)
			{
				scheduler.getPriorityLevel(mockCall("A"));
			}
			for (int i_1 = 0; i_1 < 8; i_1++)
			{
				scheduler.getPriorityLevel(mockCall("B"));
			}
			NUnit.Framework.Assert.AreEqual(12, scheduler.getTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(4, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(8, scheduler.getCallCountSnapshot()["B"]);
			scheduler.forceDecay();
			NUnit.Framework.Assert.AreEqual(6, scheduler.getTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(2, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(4, scheduler.getCallCountSnapshot()["B"]);
			scheduler.forceDecay();
			NUnit.Framework.Assert.AreEqual(3, scheduler.getTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(1, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(2, scheduler.getCallCountSnapshot()["B"]);
			scheduler.forceDecay();
			NUnit.Framework.Assert.AreEqual(1, scheduler.getTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(null, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(1, scheduler.getCallCountSnapshot()["B"]);
			scheduler.forceDecay();
			NUnit.Framework.Assert.AreEqual(0, scheduler.getTotalCallSnapshot());
			NUnit.Framework.Assert.AreEqual(null, scheduler.getCallCountSnapshot()["A"]);
			NUnit.Framework.Assert.AreEqual(null, scheduler.getCallCountSnapshot()["B"]);
		}

		[NUnit.Framework.Test]
		public virtual void testPriority()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY
				, "99999999");
			// Never flush
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY
				, "25, 50, 75");
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(4, "ns", conf);
			NUnit.Framework.Assert.AreEqual(0, scheduler.getPriorityLevel(mockCall("A")));
			NUnit.Framework.Assert.AreEqual(2, scheduler.getPriorityLevel(mockCall("A")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.getPriorityLevel(mockCall("B")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.getPriorityLevel(mockCall("B")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.getPriorityLevel(mockCall("C")));
			NUnit.Framework.Assert.AreEqual(0, scheduler.getPriorityLevel(mockCall("C")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.getPriorityLevel(mockCall("A")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.getPriorityLevel(mockCall("A")));
			NUnit.Framework.Assert.AreEqual(1, scheduler.getPriorityLevel(mockCall("A")));
			NUnit.Framework.Assert.AreEqual(2, scheduler.getPriorityLevel(mockCall("A")));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPeriodic()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY
				, "10");
			conf.set("ns." + org.apache.hadoop.ipc.DecayRpcScheduler.IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY
				, "0.5");
			scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(1, "ns", conf);
			NUnit.Framework.Assert.AreEqual(10, scheduler.getDecayPeriodMillis());
			NUnit.Framework.Assert.AreEqual(0, scheduler.getTotalCallSnapshot());
			for (int i = 0; i < 64; i++)
			{
				scheduler.getPriorityLevel(mockCall("A"));
			}
			// It should eventually decay to zero
			while (scheduler.getTotalCallSnapshot() > 0)
			{
				java.lang.Thread.sleep(10);
			}
		}
	}
}
