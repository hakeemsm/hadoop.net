using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Client
{
	public class ClientSCMProtocolPBClientImpl : ClientSCMProtocol, IDisposable
	{
		private ClientSCMProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ClientSCMProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ClientSCMProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = RPC.GetProxy<ClientSCMProtocolPB>(clientVersion, addr, conf);
		}

		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
				this.proxy = null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual UseSharedCacheResourceResponse Use(UseSharedCacheResourceRequest request
			)
		{
			YarnServiceProtos.UseSharedCacheResourceRequestProto requestProto = ((UseSharedCacheResourceRequestPBImpl
				)request).GetProto();
			try
			{
				return new UseSharedCacheResourceResponsePBImpl(proxy.Use(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReleaseSharedCacheResourceResponse Release(ReleaseSharedCacheResourceRequest
			 request)
		{
			YarnServiceProtos.ReleaseSharedCacheResourceRequestProto requestProto = ((ReleaseSharedCacheResourceRequestPBImpl
				)request).GetProto();
			try
			{
				return new ReleaseSharedCacheResourceResponsePBImpl(proxy.Release(null, requestProto
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
