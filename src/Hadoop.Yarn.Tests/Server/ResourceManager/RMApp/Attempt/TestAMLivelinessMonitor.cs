using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class TestAMLivelinessMonitor
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestResetTimer()
		{
			YarnConfiguration conf = new YarnConfiguration();
			UserGroupInformation.SetConfiguration(conf);
			conf.Set(YarnConfiguration.RecoveryEnabled, "true");
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, true);
			conf.SetInt(YarnConfiguration.RmAmExpiryIntervalMs, 6000);
			ControlledClock clock = new ControlledClock(new SystemClock());
			clock.SetTime(0);
			MemoryRMStateStore memStore = new _MemoryRMStateStore_46(clock);
			memStore.Init(conf);
			ApplicationAttemptId attemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptId>();
			Dispatcher dispatcher = Org.Mockito.Mockito.Mock<Dispatcher>();
			bool[] expired = new bool[] { false };
			AMLivelinessMonitor monitor = new _AMLivelinessMonitor_58(attemptId, expired, dispatcher
				, clock);
			monitor.Register(attemptId);
			MockRM rm = new _MockRM_66(monitor, conf, memStore);
			rm.Start();
			// make sure that monitor has started
			while (monitor.GetServiceState() != Service.STATE.Started)
			{
				Sharpen.Thread.Sleep(100);
			}
			// expired[0] would be set to true without resetTimer
			NUnit.Framework.Assert.IsFalse(expired[0]);
			rm.Stop();
		}

		private sealed class _MemoryRMStateStore_46 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_46(ControlledClock clock)
			{
				this.clock = clock;
			}

			/// <exception cref="System.Exception"/>
			public override RMStateStore.RMState LoadState()
			{
				lock (this)
				{
					clock.SetTime(8000);
					return base.LoadState();
				}
			}

			private readonly ControlledClock clock;
		}

		private sealed class _AMLivelinessMonitor_58 : AMLivelinessMonitor
		{
			public _AMLivelinessMonitor_58(ApplicationAttemptId attemptId, bool[] expired, Dispatcher
				 baseArg1, Clock baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.attemptId = attemptId;
				this.expired = expired;
			}

			protected override void Expire(ApplicationAttemptId id)
			{
				NUnit.Framework.Assert.AreEqual(id, attemptId);
				expired[0] = true;
			}

			private readonly ApplicationAttemptId attemptId;

			private readonly bool[] expired;
		}

		private sealed class _MockRM_66 : MockRM
		{
			public _MockRM_66(AMLivelinessMonitor monitor, Configuration baseArg1, RMStateStore
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.monitor = monitor;
			}

			protected internal override AMLivelinessMonitor CreateAMLivelinessMonitor()
			{
				return monitor;
			}

			private readonly AMLivelinessMonitor monitor;
		}
	}
}
