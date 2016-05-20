using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Harness for starting two dummy ZK FailoverControllers, associated with
	/// DummyHAServices.
	/// </summary>
	/// <remarks>
	/// Harness for starting two dummy ZK FailoverControllers, associated with
	/// DummyHAServices. This harness starts two such ZKFCs, designated by
	/// indexes 0 and 1, and provides utilities for building tests around them.
	/// </remarks>
	public class MiniZKFCCluster
	{
		private readonly org.apache.hadoop.test.MultithreadedTestUtil.TestContext ctx;

		private readonly org.apache.zookeeper.server.ZooKeeperServer zks;

		private org.apache.hadoop.ha.DummyHAService[] svcs;

		private org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFCThread[] thrs;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.ha.DummySharedResource sharedResource = new org.apache.hadoop.ha.DummySharedResource
			();

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.MiniZKFCCluster
			)));

		public MiniZKFCCluster(org.apache.hadoop.conf.Configuration conf, org.apache.zookeeper.server.ZooKeeperServer
			 zks)
		{
			this.conf = conf;
			// Fast check interval so tests run faster
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY
				, 50);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY
				, 50);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY
				, 50);
			svcs = new org.apache.hadoop.ha.DummyHAService[2];
			svcs[0] = new org.apache.hadoop.ha.DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.INITIALIZING, new java.net.InetSocketAddress("svc1", 1234));
			svcs[0].setSharedResource(sharedResource);
			svcs[1] = new org.apache.hadoop.ha.DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.INITIALIZING, new java.net.InetSocketAddress("svc2", 1234));
			svcs[1].setSharedResource(sharedResource);
			this.ctx = new org.apache.hadoop.test.MultithreadedTestUtil.TestContext();
			this.zks = zks;
		}

		/// <summary>Set up two services and their failover controllers.</summary>
		/// <remarks>
		/// Set up two services and their failover controllers. svc1 is started
		/// first, so that it enters ACTIVE state, and then svc2 is started,
		/// which enters STANDBY
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void start()
		{
			// Format the base dir, should succeed
			thrs = new org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFCThread[2];
			thrs[0] = new org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFCThread(this, ctx, svcs
				[0]);
			NUnit.Framework.Assert.AreEqual(0, thrs[0].zkfc.run(new string[] { "-formatZK" })
				);
			ctx.addThread(thrs[0]);
			thrs[0].start();
			LOG.info("Waiting for svc0 to enter active state");
			waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE);
			LOG.info("Adding svc1");
			thrs[1] = new org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFCThread(this, ctx, svcs
				[1]);
			thrs[1].start();
			waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY);
		}

		/// <summary>Stop the services.</summary>
		/// <exception cref="System.Exception">if either of the services had encountered a fatal error
		/// 	</exception>
		public virtual void stop()
		{
			foreach (org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFCThread thr in thrs)
			{
				if (thr != null)
				{
					thr.interrupt();
				}
			}
			if (ctx != null)
			{
				ctx.stop();
			}
			sharedResource.assertNoViolations();
		}

		/// <returns>
		/// the TestContext implementation used internally. This allows more
		/// threads to be added to the context, etc.
		/// </returns>
		public virtual org.apache.hadoop.test.MultithreadedTestUtil.TestContext getTestContext
			()
		{
			return ctx;
		}

		public virtual org.apache.hadoop.ha.DummyHAService getService(int i)
		{
			return svcs[i];
		}

		public virtual org.apache.hadoop.ha.ActiveStandbyElector getElector(int i)
		{
			return thrs[i].zkfc.getElectorForTests();
		}

		public virtual org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC getZkfc(int i)
		{
			return thrs[i].zkfc;
		}

		public virtual void setHealthy(int idx, bool healthy)
		{
			svcs[idx].isHealthy = healthy;
		}

		public virtual void setFailToBecomeActive(int idx, bool doFail)
		{
			svcs[idx].failToBecomeActive = doFail;
		}

		public virtual void setFailToBecomeStandby(int idx, bool doFail)
		{
			svcs[idx].failToBecomeStandby = doFail;
		}

		public virtual void setFailToFence(int idx, bool doFail)
		{
			svcs[idx].failToFence = doFail;
		}

		public virtual void setUnreachable(int idx, bool unreachable)
		{
			svcs[idx].actUnreachable = unreachable;
		}

		/// <summary>Wait for the given HA service to enter the given HA state.</summary>
		/// <remarks>
		/// Wait for the given HA service to enter the given HA state.
		/// This is based on the state of ZKFC, not the state of HA service.
		/// There could be difference between the two. For example,
		/// When the service becomes unhealthy, ZKFC will quit ZK election and
		/// transition to HAServiceState.INITIALIZING and remain in that state
		/// until the service becomes healthy.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void waitForHAState(int idx, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
			 state)
		{
			org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC svc = getZkfc(idx);
			while (svc.getServiceState() != state)
			{
				ctx.checkException();
				java.lang.Thread.sleep(50);
			}
		}

		/// <summary>Wait for the ZKFC to be notified of a change in health state.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void waitForHealthState(int idx, org.apache.hadoop.ha.HealthMonitor.State
			 state)
		{
			org.apache.hadoop.ha.ZKFCTestUtil.waitForHealthState(thrs[idx].zkfc, state, ctx);
		}

		/// <summary>Wait for the given elector to enter the given elector state.</summary>
		/// <param name="idx">the service index (0 or 1)</param>
		/// <param name="state">the state to wait for</param>
		/// <exception cref="System.Exception">
		/// if it times out, or an exception occurs on one
		/// of the ZKFC threads while waiting.
		/// </exception>
		public virtual void waitForElectorState(int idx, org.apache.hadoop.ha.ActiveStandbyElector.State
			 state)
		{
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForElectorState(ctx, getElector
				(idx), state);
		}

		/// <summary>Expire the ZK session of the given service.</summary>
		/// <remarks>
		/// Expire the ZK session of the given service. This requires
		/// (and asserts) that the given service be the current active.
		/// </remarks>
		/// <exception cref="org.apache.zookeeper.KeeperException.NoNodeException">if no service holds the lock
		/// 	</exception>
		public virtual void expireActiveLockHolder(int idx)
		{
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			byte[] data = zks.getZKDatabase().getData(org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC
				.LOCK_ZNODE, stat, null);
			NUnit.Framework.Assert.assertArrayEquals(com.google.common.primitives.Ints.toByteArray
				(svcs[idx].index), data);
			long session = stat.getEphemeralOwner();
			LOG.info("Expiring svc " + idx + "'s zookeeper session " + session);
			zks.closeSession(session);
		}

		/// <summary>Wait for the given HA service to become the active lock holder.</summary>
		/// <remarks>
		/// Wait for the given HA service to become the active lock holder.
		/// If the passed svc is null, waits for there to be no active
		/// lock holder.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void waitForActiveLockHolder(int idx)
		{
			org.apache.hadoop.ha.DummyHAService svc = idx == null ? null : svcs[idx];
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(ctx, zks, 
				org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC.SCOPED_PARENT_ZNODE, (idx == null
				) ? null : com.google.common.primitives.Ints.toByteArray(svc.index));
		}

		/// <summary>
		/// Expires the ZK session associated with service 'fromIdx', and waits
		/// until service 'toIdx' takes over.
		/// </summary>
		/// <exception cref="System.Exception">if the target service does not become active</exception>
		public virtual void expireAndVerifyFailover(int fromIdx, int toIdx)
		{
			com.google.common.@base.Preconditions.checkArgument(fromIdx != toIdx);
			getElector(fromIdx).preventSessionReestablishmentForTests();
			try
			{
				expireActiveLockHolder(fromIdx);
				waitForHAState(fromIdx, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY
					);
				waitForHAState(toIdx, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
			}
			finally
			{
				getElector(fromIdx).allowSessionReestablishmentForTests();
			}
		}

		/// <summary>
		/// Test-thread which runs a ZK Failover Controller corresponding
		/// to a given dummy service.
		/// </summary>
		private class DummyZKFCThread : org.apache.hadoop.test.MultithreadedTestUtil.TestingThread
		{
			private readonly org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC zkfc;

			public DummyZKFCThread(MiniZKFCCluster _enclosing, org.apache.hadoop.test.MultithreadedTestUtil.TestContext
				 ctx, org.apache.hadoop.ha.DummyHAService svc)
				: base(ctx)
			{
				this._enclosing = _enclosing;
				this.zkfc = new org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC(this._enclosing.conf
					, svc);
			}

			/// <exception cref="System.Exception"/>
			public override void doWork()
			{
				try
				{
					NUnit.Framework.Assert.AreEqual(0, this.zkfc.run(new string[0]));
				}
				catch (System.Exception)
				{
				}
			}

			private readonly MiniZKFCCluster _enclosing;
			// Interrupted by main thread, that's OK.
		}

		internal class DummyZKFC : org.apache.hadoop.ha.ZKFailoverController
		{
			private const string DUMMY_CLUSTER = "dummy-cluster";

			public const string SCOPED_PARENT_ZNODE = org.apache.hadoop.ha.ZKFailoverController
				.ZK_PARENT_ZNODE_DEFAULT + "/" + DUMMY_CLUSTER;

			private const string LOCK_ZNODE = SCOPED_PARENT_ZNODE + "/" + org.apache.hadoop.ha.ActiveStandbyElector
				.LOCK_FILENAME;

			private readonly org.apache.hadoop.ha.DummyHAService localTarget;

			public DummyZKFC(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.ha.DummyHAService
				 localTarget)
				: base(conf, localTarget)
			{
				this.localTarget = localTarget;
			}

			protected internal override byte[] targetToData(org.apache.hadoop.ha.HAServiceTarget
				 target)
			{
				return com.google.common.primitives.Ints.toByteArray(((org.apache.hadoop.ha.DummyHAService
					)target).index);
			}

			protected internal override org.apache.hadoop.ha.HAServiceTarget dataToTarget(byte
				[] data)
			{
				int index = com.google.common.primitives.Ints.fromByteArray(data);
				return org.apache.hadoop.ha.DummyHAService.getInstance(index);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void loginAsFCUser()
			{
			}

			protected internal override string getScopeInsideParentNode()
			{
				return DUMMY_CLUSTER;
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			protected internal override void checkRpcAdminAccess()
			{
			}

			protected internal override java.net.InetSocketAddress getRpcAddressToBindTo()
			{
				return new java.net.InetSocketAddress(0);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void initRPC()
			{
				base.initRPC();
				localTarget.zkfcProxy = this.getRpcServerForTests();
			}

			protected internal override org.apache.hadoop.security.authorize.PolicyProvider getPolicyProvider
				()
			{
				return null;
			}
		}
	}
}
