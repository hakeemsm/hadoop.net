using System.Net;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.HA.ProtocolPB;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Authorize;


namespace Org.Apache.Hadoop.HA
{
	public class ZKFCRpcServer : ZKFCProtocol
	{
		private const int HandlerCount = 3;

		private readonly ZKFailoverController zkfc;

		private RPC.Server server;

		/// <exception cref="System.IO.IOException"/>
		internal ZKFCRpcServer(Configuration conf, IPEndPoint bindAddr, ZKFailoverController
			 zkfc, PolicyProvider policy)
		{
			this.zkfc = zkfc;
			RPC.SetProtocolEngine(conf, typeof(ZKFCProtocolPB), typeof(ProtobufRpcEngine));
			ZKFCProtocolServerSideTranslatorPB translator = new ZKFCProtocolServerSideTranslatorPB
				(this);
			BlockingService service = ZKFCProtocolProtos.ZKFCProtocolService.NewReflectiveBlockingService
				(translator);
			this.server = new RPC.Builder(conf).SetProtocol(typeof(ZKFCProtocolPB)).SetInstance
				(service).SetBindAddress(bindAddr.GetHostName()).SetPort(bindAddr.Port).SetNumHandlers
				(HandlerCount).SetVerbose(false).Build();
			// set service-level authorization security policy
			if (conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, false))
			{
				server.RefreshServiceAcl(conf, policy);
			}
		}

		internal virtual void Start()
		{
			this.server.Start();
		}

		public virtual IPEndPoint GetAddress()
		{
			return server.GetListenerAddress();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void StopAndJoin()
		{
			this.server.Stop();
			this.server.Join();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public override void CedeActive(int millisToCede)
		{
			zkfc.CheckRpcAdminAccess();
			zkfc.CedeActive(millisToCede);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public override void GracefulFailover()
		{
			zkfc.CheckRpcAdminAccess();
			zkfc.GracefulFailoverToYou();
		}
	}
}
