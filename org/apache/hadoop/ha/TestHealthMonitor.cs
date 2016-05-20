using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestHealthMonitor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.TestHealthMonitor
			)));

		/// <summary>How many times has createProxy been called</summary>
		private java.util.concurrent.atomic.AtomicInteger createProxyCount = new java.util.concurrent.atomic.AtomicInteger
			(0);

		private volatile bool throwOOMEOnCreate = false;

		private org.apache.hadoop.ha.HealthMonitor hm;

		private org.apache.hadoop.ha.DummyHAService svc;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setupHM()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
				, 1);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY
				, 50);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY
				, 50);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY
				, 50);
			svc = new org.apache.hadoop.ha.DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, new java.net.InetSocketAddress("0.0.0.0", 0), true);
			hm = new _HealthMonitor_60(this, conf, svc);
			LOG.info("Starting health monitor");
			hm.start();
			LOG.info("Waiting for HEALTHY signal");
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY);
		}

		private sealed class _HealthMonitor_60 : org.apache.hadoop.ha.HealthMonitor
		{
			public _HealthMonitor_60(TestHealthMonitor _enclosing, org.apache.hadoop.conf.Configuration
				 baseArg1, org.apache.hadoop.ha.HAServiceTarget baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override org.apache.hadoop.ha.HAServiceProtocol createProxy()
			{
				this._enclosing.createProxyCount.incrementAndGet();
				if (this._enclosing.throwOOMEOnCreate)
				{
					throw new System.OutOfMemoryException("oome");
				}
				return base.createProxy();
			}

			private readonly TestHealthMonitor _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMonitor()
		{
			LOG.info("Mocking bad health check, waiting for UNHEALTHY");
			svc.isHealthy = false;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_UNHEALTHY);
			LOG.info("Returning to healthy state, waiting for HEALTHY");
			svc.isHealthy = true;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY);
			LOG.info("Returning an IOException, as if node went down");
			// should expect many rapid retries
			int countBefore = createProxyCount.get();
			svc.actUnreachable = true;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_NOT_RESPONDING);
			// Should retry several times
			while (createProxyCount.get() < countBefore + 3)
			{
				java.lang.Thread.sleep(10);
			}
			LOG.info("Returning to healthy state, waiting for HEALTHY");
			svc.actUnreachable = false;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY);
			hm.shutdown();
			hm.join();
			NUnit.Framework.Assert.IsFalse(hm.isAlive());
		}

		/// <summary>
		/// Test that the proper state is propagated when the health monitor
		/// sees an uncaught exception in its thread.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testHealthMonitorDies()
		{
			LOG.info("Mocking RTE in health monitor, waiting for FAILED");
			throwOOMEOnCreate = true;
			svc.actUnreachable = true;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.HEALTH_MONITOR_FAILED);
			hm.shutdown();
			hm.join();
			NUnit.Framework.Assert.IsFalse(hm.isAlive());
		}

		/// <summary>
		/// Test that, if the callback throws an RTE, this will terminate the
		/// health monitor and thus change its state to FAILED
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testCallbackThrowsRTE()
		{
			hm.addCallback(new _Callback_129());
			LOG.info("Mocking bad health check, waiting for UNHEALTHY");
			svc.isHealthy = false;
			waitForState(hm, org.apache.hadoop.ha.HealthMonitor.State.HEALTH_MONITOR_FAILED);
		}

		private sealed class _Callback_129 : org.apache.hadoop.ha.HealthMonitor.Callback
		{
			public _Callback_129()
			{
			}

			public void enteredState(org.apache.hadoop.ha.HealthMonitor.State newState)
			{
				throw new System.Exception("Injected RTE");
			}
		}

		/// <exception cref="System.Exception"/>
		private void waitForState(org.apache.hadoop.ha.HealthMonitor hm, org.apache.hadoop.ha.HealthMonitor.State
			 state)
		{
			long st = org.apache.hadoop.util.Time.now();
			while (org.apache.hadoop.util.Time.now() - st < 2000)
			{
				if (hm.getHealthState() == state)
				{
					return;
				}
				java.lang.Thread.sleep(50);
			}
			NUnit.Framework.Assert.AreEqual(state, hm.getHealthState());
		}
	}
}
