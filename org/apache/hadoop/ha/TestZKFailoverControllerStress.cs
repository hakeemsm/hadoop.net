using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>Stress test for ZKFailoverController.</summary>
	/// <remarks>
	/// Stress test for ZKFailoverController.
	/// Starts multiple ZKFCs for dummy services, and then performs many automatic
	/// failovers. While doing so, ensures that a fake "shared resource"
	/// (simulating the shared edits dir) is only owned by one service at a time.
	/// </remarks>
	public class TestZKFailoverControllerStress : org.apache.hadoop.ha.ClientBaseWithFixes
	{
		private const int STRESS_RUNTIME_SECS = 30;

		private const int EXTRA_TIMEOUT_SECS = 10;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.ha.MiniZKFCCluster cluster;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setupConfAndServices()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.set(org.apache.hadoop.ha.ZKFailoverController.ZK_QUORUM_KEY, hostPort);
			this.cluster = new org.apache.hadoop.ha.MiniZKFCCluster(conf, getServer(serverFactory
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void stopCluster()
		{
			if (cluster != null)
			{
				cluster.stop();
			}
		}

		/// <summary>
		/// Simply fail back and forth between two services for the
		/// configured amount of time, via expiring their ZK sessions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testExpireBackAndForth()
		{
			cluster.start();
			long st = org.apache.hadoop.util.Time.now();
			long runFor = STRESS_RUNTIME_SECS * 1000;
			int i = 0;
			while (org.apache.hadoop.util.Time.now() - st < runFor)
			{
				// flip flop the services back and forth
				int from = i % 2;
				int to = (i + 1) % 2;
				// Expire one service, it should fail over to the other
				LOG.info("Failing over via expiration from " + from + " to " + to);
				cluster.expireAndVerifyFailover(from, to);
				i++;
			}
		}

		/// <summary>Randomly expire the ZK sessions of the two ZKFCs.</summary>
		/// <remarks>
		/// Randomly expire the ZK sessions of the two ZKFCs. This differs
		/// from the above test in that it is not a controlled failover -
		/// we just do random expirations and expect neither one to ever
		/// generate fatal exceptions.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomExpirations()
		{
			cluster.start();
			long st = org.apache.hadoop.util.Time.now();
			long runFor = STRESS_RUNTIME_SECS * 1000;
			java.util.Random r = new java.util.Random();
			while (org.apache.hadoop.util.Time.now() - st < runFor)
			{
				cluster.getTestContext().checkException();
				int targetIdx = r.nextInt(2);
				org.apache.hadoop.ha.ActiveStandbyElector target = cluster.getElector(targetIdx);
				long sessId = target.getZKSessionIdForTests();
				if (sessId != -1)
				{
					LOG.info(string.format("Expiring session %x for svc %d", sessId, targetIdx));
					getServer(serverFactory).closeSession(sessId);
				}
				java.lang.Thread.sleep(r.nextInt(300));
			}
		}

		/// <summary>
		/// Have the services fail their health checks half the time,
		/// causing the master role to bounce back and forth in the
		/// cluster.
		/// </summary>
		/// <remarks>
		/// Have the services fail their health checks half the time,
		/// causing the master role to bounce back and forth in the
		/// cluster. Meanwhile, causes ZK to disconnect clients every
		/// 50ms, to trigger the retry code and failures to become active.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomHealthAndDisconnects()
		{
			long runFor = STRESS_RUNTIME_SECS * 1000;
			org.mockito.Mockito.doAnswer(new org.apache.hadoop.ha.TestZKFailoverControllerStress.RandomlyThrow
				(0)).when(cluster.getService(0).proxy).monitorHealth();
			org.mockito.Mockito.doAnswer(new org.apache.hadoop.ha.TestZKFailoverControllerStress.RandomlyThrow
				(1)).when(cluster.getService(1).proxy).monitorHealth();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY
				, 100);
			// Don't start until after the above mocking. Otherwise we can get
			// Mockito errors if the HM calls the proxy in the middle of
			// setting up the mock.
			cluster.start();
			long st = org.apache.hadoop.util.Time.now();
			while (org.apache.hadoop.util.Time.now() - st < runFor)
			{
				cluster.getTestContext().checkException();
				serverFactory.closeAll();
				java.lang.Thread.sleep(50);
			}
		}

		/// <summary>Randomly throw an exception half the time the method is called</summary>
		private class RandomlyThrow : org.mockito.stubbing.Answer
		{
			private java.util.Random r = new java.util.Random();

			private readonly int svcIdx;

			public RandomlyThrow(int svcIdx)
			{
				this.svcIdx = svcIdx;
			}

			/// <exception cref="System.Exception"/>
			public virtual object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				if (r.nextBoolean())
				{
					LOG.info("Throwing an exception for svc " + svcIdx);
					throw new org.apache.hadoop.ha.HealthCheckFailedException("random failure");
				}
				return invocation.callRealMethod();
			}
		}
	}
}
