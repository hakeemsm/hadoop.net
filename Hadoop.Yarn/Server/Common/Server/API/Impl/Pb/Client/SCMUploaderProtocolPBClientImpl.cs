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
	public class SCMUploaderProtocolPBClientImpl : SCMUploaderProtocol, IDisposable
	{
		private SCMUploaderProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public SCMUploaderProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(SCMUploaderProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = RPC.GetProxy<SCMUploaderProtocolPB>(clientVersion, addr, conf);
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
		public virtual SCMUploaderNotifyResponse Notify(SCMUploaderNotifyRequest request)
		{
			YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto requestProto = ((SCMUploaderNotifyRequestPBImpl
				)request).GetProto();
			try
			{
				return new SCMUploaderNotifyResponsePBImpl(proxy.Notify(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SCMUploaderCanUploadResponse CanUpload(SCMUploaderCanUploadRequest
			 request)
		{
			YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto requestProto = ((SCMUploaderCanUploadRequestPBImpl
				)request).GetProto();
			try
			{
				return new SCMUploaderCanUploadResponsePBImpl(proxy.CanUpload(null, requestProto)
					);
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
