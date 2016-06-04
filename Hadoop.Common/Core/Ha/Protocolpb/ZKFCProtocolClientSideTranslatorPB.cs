using System;
using System.Net;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.HA.ProtocolPB
{
	public class ZKFCProtocolClientSideTranslatorPB : ZKFCProtocol, IDisposable, ProtocolTranslator
	{
		private static readonly RpcController NullController = null;

		private readonly ZKFCProtocolPB rpcProxy;

		/// <exception cref="System.IO.IOException"/>
		public ZKFCProtocolClientSideTranslatorPB(IPEndPoint addr, Configuration conf, SocketFactory
			 socketFactory, int timeout)
		{
			RPC.SetProtocolEngine(conf, typeof(ZKFCProtocolPB), typeof(ProtobufRpcEngine));
			rpcProxy = RPC.GetProxy<ZKFCProtocolPB>(RPC.GetProtocolVersion(typeof(ZKFCProtocolPB
				)), addr, UserGroupInformation.GetCurrentUser(), conf, socketFactory, timeout);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void CedeActive(int millisToCede)
		{
			try
			{
				ZKFCProtocolProtos.CedeActiveRequestProto req = ((ZKFCProtocolProtos.CedeActiveRequestProto
					)ZKFCProtocolProtos.CedeActiveRequestProto.NewBuilder().SetMillisToCede(millisToCede
					).Build());
				rpcProxy.CedeActive(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void GracefulFailover()
		{
			try
			{
				rpcProxy.GracefulFailover(NullController, ZKFCProtocolProtos.GracefulFailoverRequestProto
					.GetDefaultInstance());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
