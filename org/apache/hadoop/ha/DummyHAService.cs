using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Test-only implementation of
	/// <see cref="HAServiceTarget"/>
	/// , which returns
	/// a mock implementation.
	/// </summary>
	internal class DummyHAService : org.apache.hadoop.ha.HAServiceTarget
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.DummyHAService
			)));

		private const string DUMMY_FENCE_KEY = "dummy.fence.key";

		internal volatile org.apache.hadoop.ha.HAServiceProtocol.HAServiceState state;

		internal org.apache.hadoop.ha.HAServiceProtocol proxy;

		internal org.apache.hadoop.ha.ZKFCProtocol zkfcProxy = null;

		internal org.apache.hadoop.ha.NodeFencer fencer;

		internal java.net.InetSocketAddress address;

		internal bool isHealthy = true;

		internal bool actUnreachable = false;

		internal bool failToBecomeActive;

		internal bool failToBecomeStandby;

		internal bool failToFence;

		internal org.apache.hadoop.ha.DummySharedResource sharedResource;

		public int fenceCount = 0;

		public int activeTransitionCount = 0;

		internal bool testWithProtoBufRPC = false;

		internal static System.Collections.Generic.List<org.apache.hadoop.ha.DummyHAService
			> instances = com.google.common.collect.Lists.newArrayList();

		internal int index;

		internal DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState state
			, java.net.InetSocketAddress address)
			: this(state, address, false)
		{
		}

		internal DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState state
			, java.net.InetSocketAddress address, bool testWithProtoBufRPC)
		{
			this.state = state;
			this.testWithProtoBufRPC = testWithProtoBufRPC;
			if (testWithProtoBufRPC)
			{
				this.address = startAndGetRPCServerAddress(address);
			}
			else
			{
				this.address = address;
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			this.proxy = makeMock(conf, org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT
				);
			try
			{
				conf.set(DUMMY_FENCE_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.DummyHAService.DummyFencer
					)).getName());
				this.fencer = org.mockito.Mockito.spy(org.apache.hadoop.ha.NodeFencer.create(conf
					, DUMMY_FENCE_KEY));
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException e)
			{
				throw new System.Exception(e);
			}
			lock (instances)
			{
				instances.add(this);
				this.index = instances.Count;
			}
		}

		public virtual void setSharedResource(org.apache.hadoop.ha.DummySharedResource rsrc
			)
		{
			this.sharedResource = rsrc;
		}

		private java.net.InetSocketAddress startAndGetRPCServerAddress(java.net.InetSocketAddress
			 serverAddress)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			try
			{
				org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
				org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator
					 = new org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB(new 
					org.apache.hadoop.ha.DummyHAService.MockHAProtocolImpl(this));
				com.google.protobuf.BlockingService haPbService = org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService
					.newReflectiveBlockingService(haServiceProtocolXlator);
				org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
					.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB
					))).setInstance(haPbService).setBindAddress(serverAddress.getHostName()).setPort
					(serverAddress.getPort()).build();
				server.start();
				return org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			}
			catch (System.IO.IOException)
			{
				return null;
			}
		}

		private org.apache.hadoop.ha.HAServiceProtocol makeMock(org.apache.hadoop.conf.Configuration
			 conf, int timeoutMs)
		{
			org.apache.hadoop.ha.HAServiceProtocol service;
			if (!testWithProtoBufRPC)
			{
				service = new org.apache.hadoop.ha.DummyHAService.MockHAProtocolImpl(this);
			}
			else
			{
				try
				{
					service = base.getProxy(conf, timeoutMs);
				}
				catch (System.IO.IOException)
				{
					return null;
				}
			}
			return org.mockito.Mockito.spy(service);
		}

		public override java.net.InetSocketAddress getAddress()
		{
			return address;
		}

		public override java.net.InetSocketAddress getZKFCAddress()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.ha.HAServiceProtocol getProxy(org.apache.hadoop.conf.Configuration
			 conf, int timeout)
		{
			if (testWithProtoBufRPC)
			{
				proxy = makeMock(conf, timeout);
			}
			return proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.ha.ZKFCProtocol getZKFCProxy(org.apache.hadoop.conf.Configuration
			 conf, int timeout)
		{
			System.Diagnostics.Debug.Assert(zkfcProxy != null);
			return zkfcProxy;
		}

		public override org.apache.hadoop.ha.NodeFencer getFencer()
		{
			return fencer;
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public override void checkFencingConfigured()
		{
		}

		public override bool isAutoFailoverEnabled()
		{
			return true;
		}

		public override string ToString()
		{
			return "DummyHAService #" + index;
		}

		public static org.apache.hadoop.ha.HAServiceTarget getInstance(int serial)
		{
			return instances[serial - 1];
		}

		private class MockHAProtocolImpl : org.apache.hadoop.ha.HAServiceProtocol, java.io.Closeable
		{
			/// <exception cref="org.apache.hadoop.ha.HealthCheckFailedException"/>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void monitorHealth()
			{
				this.checkUnreachable();
				if (!this._enclosing.isHealthy)
				{
					throw new org.apache.hadoop.ha.HealthCheckFailedException("not healthy");
				}
			}

			/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void transitionToActive(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
				 req)
			{
				this._enclosing.activeTransitionCount++;
				this.checkUnreachable();
				if (this._enclosing.failToBecomeActive)
				{
					throw new org.apache.hadoop.ha.ServiceFailedException("injected failure");
				}
				if (this._enclosing.sharedResource != null)
				{
					this._enclosing.sharedResource.take(this._enclosing);
				}
				this._enclosing.state = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
			}

			/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void transitionToStandby(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
				 req)
			{
				this.checkUnreachable();
				if (this._enclosing.failToBecomeStandby)
				{
					throw new org.apache.hadoop.ha.ServiceFailedException("injected failure");
				}
				if (this._enclosing.sharedResource != null)
				{
					this._enclosing.sharedResource.release(this._enclosing);
				}
				this._enclosing.state = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ha.HAServiceStatus getServiceStatus()
			{
				this.checkUnreachable();
				org.apache.hadoop.ha.HAServiceStatus ret = new org.apache.hadoop.ha.HAServiceStatus
					(this._enclosing.state);
				if (this._enclosing.state == org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
					.STANDBY || this._enclosing.state == org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
					.ACTIVE)
				{
					ret.setReadyToBecomeActive();
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			private void checkUnreachable()
			{
				if (this._enclosing.actUnreachable)
				{
					throw new System.IO.IOException("Connection refused (fake)");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
			}

			internal MockHAProtocolImpl(DummyHAService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DummyHAService _enclosing;
		}

		public class DummyFencer : org.apache.hadoop.ha.FenceMethod
		{
			/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
			public virtual void checkArgs(string args)
			{
			}

			/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
			public virtual bool tryFence(org.apache.hadoop.ha.HAServiceTarget target, string 
				args)
			{
				LOG.info("tryFence(" + target + ")");
				org.apache.hadoop.ha.DummyHAService svc = (org.apache.hadoop.ha.DummyHAService)target;
				lock (svc)
				{
					svc.fenceCount++;
				}
				if (svc.failToFence)
				{
					LOG.info("Injected failure to fence");
					return false;
				}
				svc.sharedResource.release(svc);
				return true;
			}
		}
	}
}
