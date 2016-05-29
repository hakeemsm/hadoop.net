using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMEmbeddedElector : ClientBaseWithFixes
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestRMEmbeddedElector)
			.FullName);

		private const string Rm1NodeId = "rm1";

		private const int Rm1PortBase = 10000;

		private const string Rm2NodeId = "rm2";

		private const int Rm2PortBase = 20000;

		private Configuration conf;

		private AtomicBoolean callbackCalled;

		private void SetConfForRM(string rmId, string prefix, string value)
		{
			conf.Set(HAUtil.AddSuffix(prefix, rmId), value);
		}

		private void SetRpcAddressForRM(string rmId, int @base)
		{
			SetConfForRM(rmId, YarnConfiguration.RmAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmPort));
			SetConfForRM(rmId, YarnConfiguration.RmSchedulerAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmSchedulerPort));
			SetConfForRM(rmId, YarnConfiguration.RmAdminAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmAdminPort));
			SetConfForRM(rmId, YarnConfiguration.RmResourceTrackerAddress, "0.0.0.0:" + (@base
				 + YarnConfiguration.DefaultRmResourceTrackerPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmWebappPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappHttpsAddress, "0.0.0.0:" + (@base + 
				YarnConfiguration.DefaultRmWebappHttpsPort));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, true);
			conf.SetBoolean(YarnConfiguration.AutoFailoverEmbedded, true);
			conf.Set(YarnConfiguration.RmClusterId, "yarn-test-cluster");
			conf.Set(YarnConfiguration.RmZkAddress, hostPort);
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, 2000);
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			conf.Set(YarnConfiguration.RmHaId, Rm1NodeId);
			SetRpcAddressForRM(Rm1NodeId, Rm1PortBase);
			SetRpcAddressForRM(Rm2NodeId, Rm2PortBase);
			conf.SetLong(YarnConfiguration.ClientFailoverSleeptimeBaseMs, 100L);
			callbackCalled = new AtomicBoolean(false);
		}

		/// <summary>
		/// Test that tries to see if there is a deadlock between
		/// (a) the thread stopping the RM
		/// (b) thread processing the ZK event asking RM to transition to active
		/// The test times out if there is a deadlock.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeadlockShutdownBecomeActive()
		{
			MockRM rm = new TestRMEmbeddedElector.MockRMWithElector(this, conf, 1000);
			rm.Start();
			Log.Info("Waiting for callback");
			while (!callbackCalled.Get())
			{
			}
			Log.Info("Stopping RM");
			rm.Stop();
			Log.Info("Stopped RM");
		}

		private class MockRMWithElector : MockRM
		{
			private long delayMs = 0;

			internal MockRMWithElector(TestRMEmbeddedElector _enclosing, Configuration conf)
				: base(conf)
			{
				this._enclosing = _enclosing;
			}

			internal MockRMWithElector(TestRMEmbeddedElector _enclosing, Configuration conf, 
				long delayMs)
				: this(conf)
			{
				this._enclosing = _enclosing;
				this.delayMs = delayMs;
			}

			protected internal override AdminService CreateAdminService()
			{
				return new _AdminService_116(this, this, this.GetRMContext());
			}

			private sealed class _AdminService_116 : AdminService
			{
				public _AdminService_116(MockRMWithElector _enclosing, ResourceManager baseArg1, 
					RMContext baseArg2)
					: base(baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
				}

				protected internal override EmbeddedElectorService CreateEmbeddedElectorService()
				{
					return new _EmbeddedElectorService_119(this, this._enclosing.GetRMContext());
				}

				private sealed class _EmbeddedElectorService_119 : EmbeddedElectorService
				{
					public _EmbeddedElectorService_119(_AdminService_116 _enclosing, RMContext baseArg1
						)
						: base(baseArg1)
					{
						this._enclosing = _enclosing;
					}

					/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
					public override void BecomeActive()
					{
						try
						{
							this._enclosing._enclosing._enclosing.callbackCalled.Set(true);
							TestRMEmbeddedElector.Log.Info("Callback called. Sleeping now");
							Sharpen.Thread.Sleep(this._enclosing._enclosing.delayMs);
							TestRMEmbeddedElector.Log.Info("Sleep done");
						}
						catch (Exception e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
						}
						base.BecomeActive();
					}

					private readonly _AdminService_116 _enclosing;
				}

				private readonly MockRMWithElector _enclosing;
			}

			private readonly TestRMEmbeddedElector _enclosing;
		}
	}
}
