using System;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestHealthMonitor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestHealthMonitor));

		/// <summary>How many times has createProxy been called</summary>
		private AtomicInteger createProxyCount = new AtomicInteger(0);

		private volatile bool throwOOMEOnCreate = false;

		private HealthMonitor hm;

		private DummyHAService svc;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupHM()
		{
			Configuration conf = new Configuration();
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesKey, 1);
			conf.SetInt(CommonConfigurationKeys.HaHmCheckIntervalKey, 50);
			conf.SetInt(CommonConfigurationKeys.HaHmConnectRetryIntervalKey, 50);
			conf.SetInt(CommonConfigurationKeys.HaHmSleepAfterDisconnectKey, 50);
			svc = new DummyHAService(HAServiceProtocol.HAServiceState.Active, new IPEndPoint(
				"0.0.0.0", 0), true);
			hm = new _HealthMonitor_60(this, conf, svc);
			Log.Info("Starting health monitor");
			hm.Start();
			Log.Info("Waiting for HEALTHY signal");
			WaitForState(hm, HealthMonitor.State.ServiceHealthy);
		}

		private sealed class _HealthMonitor_60 : HealthMonitor
		{
			public _HealthMonitor_60(TestHealthMonitor _enclosing, Configuration baseArg1, HAServiceTarget
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override HAServiceProtocol CreateProxy()
			{
				this._enclosing.createProxyCount.IncrementAndGet();
				if (this._enclosing.throwOOMEOnCreate)
				{
					throw new OutOfMemoryException("oome");
				}
				return base.CreateProxy();
			}

			private readonly TestHealthMonitor _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMonitor()
		{
			Log.Info("Mocking bad health check, waiting for UNHEALTHY");
			svc.isHealthy = false;
			WaitForState(hm, HealthMonitor.State.ServiceUnhealthy);
			Log.Info("Returning to healthy state, waiting for HEALTHY");
			svc.isHealthy = true;
			WaitForState(hm, HealthMonitor.State.ServiceHealthy);
			Log.Info("Returning an IOException, as if node went down");
			// should expect many rapid retries
			int countBefore = createProxyCount.Get();
			svc.actUnreachable = true;
			WaitForState(hm, HealthMonitor.State.ServiceNotResponding);
			// Should retry several times
			while (createProxyCount.Get() < countBefore + 3)
			{
				Sharpen.Thread.Sleep(10);
			}
			Log.Info("Returning to healthy state, waiting for HEALTHY");
			svc.actUnreachable = false;
			WaitForState(hm, HealthMonitor.State.ServiceHealthy);
			hm.Shutdown();
			hm.Join();
			NUnit.Framework.Assert.IsFalse(hm.IsAlive());
		}

		/// <summary>
		/// Test that the proper state is propagated when the health monitor
		/// sees an uncaught exception in its thread.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestHealthMonitorDies()
		{
			Log.Info("Mocking RTE in health monitor, waiting for FAILED");
			throwOOMEOnCreate = true;
			svc.actUnreachable = true;
			WaitForState(hm, HealthMonitor.State.HealthMonitorFailed);
			hm.Shutdown();
			hm.Join();
			NUnit.Framework.Assert.IsFalse(hm.IsAlive());
		}

		/// <summary>
		/// Test that, if the callback throws an RTE, this will terminate the
		/// health monitor and thus change its state to FAILED
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCallbackThrowsRTE()
		{
			hm.AddCallback(new _Callback_129());
			Log.Info("Mocking bad health check, waiting for UNHEALTHY");
			svc.isHealthy = false;
			WaitForState(hm, HealthMonitor.State.HealthMonitorFailed);
		}

		private sealed class _Callback_129 : HealthMonitor.Callback
		{
			public _Callback_129()
			{
			}

			public void EnteredState(HealthMonitor.State newState)
			{
				throw new RuntimeException("Injected RTE");
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForState(HealthMonitor hm, HealthMonitor.State state)
		{
			long st = Time.Now();
			while (Time.Now() - st < 2000)
			{
				if (hm.GetHealthState() == state)
				{
					return;
				}
				Sharpen.Thread.Sleep(50);
			}
			NUnit.Framework.Assert.AreEqual(state, hm.GetHealthState());
		}
	}
}
