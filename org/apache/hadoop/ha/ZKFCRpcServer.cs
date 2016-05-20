using Sharpen;

namespace org.apache.hadoop.ha
{
	public class ZKFCRpcServer : org.apache.hadoop.ha.ZKFCProtocol
	{
		private const int HANDLER_COUNT = 3;

		private readonly org.apache.hadoop.ha.ZKFailoverController zkfc;

		private org.apache.hadoop.ipc.RPC.Server server;

		/// <exception cref="System.IO.IOException"/>
		internal ZKFCRpcServer(org.apache.hadoop.conf.Configuration conf, java.net.InetSocketAddress
			 bindAddr, org.apache.hadoop.ha.ZKFailoverController zkfc, org.apache.hadoop.security.authorize.PolicyProvider
			 policy)
		{
			this.zkfc = zkfc;
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			org.apache.hadoop.ha.protocolPB.ZKFCProtocolServerSideTranslatorPB translator = new 
				org.apache.hadoop.ha.protocolPB.ZKFCProtocolServerSideTranslatorPB(this);
			com.google.protobuf.BlockingService service = org.apache.hadoop.ha.proto.ZKFCProtocolProtos.ZKFCProtocolService
				.newReflectiveBlockingService(translator);
			this.server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB))).setInstance(service).setBindAddress
				(bindAddr.getHostName()).setPort(bindAddr.getPort()).setNumHandlers(HANDLER_COUNT
				).setVerbose(false).build();
			// set service-level authorization security policy
			if (conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, false))
			{
				server.refreshServiceAcl(conf, policy);
			}
		}

		internal virtual void start()
		{
			this.server.start();
		}

		public virtual java.net.InetSocketAddress getAddress()
		{
			return server.getListenerAddress();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void stopAndJoin()
		{
			this.server.stop();
			this.server.join();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		public override void cedeActive(int millisToCede)
		{
			zkfc.checkRpcAdminAccess();
			zkfc.cedeActive(millisToCede);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		public override void gracefulFailover()
		{
			zkfc.checkRpcAdminAccess();
			zkfc.gracefulFailoverToYou();
		}
	}
}
