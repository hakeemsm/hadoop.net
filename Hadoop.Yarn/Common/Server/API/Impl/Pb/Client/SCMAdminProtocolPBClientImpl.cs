using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Impl.PB.Client
{
	public class SCMAdminProtocolPBClientImpl : SCMAdminProtocol, IDisposable
	{
		private SCMAdminProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public SCMAdminProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(SCMAdminProtocolPB), typeof(ProtobufRpcEngine)
				);
			proxy = RPC.GetProxy<SCMAdminProtocolPB>(clientVersion, addr, conf);
		}

		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RunSharedCacheCleanerTaskResponse RunCleanerTask(RunSharedCacheCleanerTaskRequest
			 request)
		{
			YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto requestProto = ((RunSharedCacheCleanerTaskRequestPBImpl
				)request).GetProto();
			try
			{
				return new RunSharedCacheCleanerTaskResponsePBImpl(proxy.RunCleanerTask(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
