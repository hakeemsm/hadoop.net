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
	public class ApplicationMasterProtocolPBClientImpl : ApplicationMasterProtocol, IDisposable
	{
		private ApplicationMasterProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ApplicationMasterProtocolPBClientImpl(long clientVersion, IPEndPoint addr, 
			Configuration conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ApplicationMasterProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = (ApplicationMasterProtocolPB)RPC.GetProxy<ApplicationMasterProtocolPB>(clientVersion
				, addr, conf);
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
		public virtual AllocateResponse Allocate(AllocateRequest request)
		{
			YarnServiceProtos.AllocateRequestProto requestProto = ((AllocateRequestPBImpl)request
				).GetProto();
			try
			{
				return new AllocateResponsePBImpl(proxy.Allocate(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
			 request)
		{
			YarnServiceProtos.FinishApplicationMasterRequestProto requestProto = ((FinishApplicationMasterRequestPBImpl
				)request).GetProto();
			try
			{
				return new FinishApplicationMasterResponsePBImpl(proxy.FinishApplicationMaster(null
					, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
			 request)
		{
			YarnServiceProtos.RegisterApplicationMasterRequestProto requestProto = ((RegisterApplicationMasterRequestPBImpl
				)request).GetProto();
			try
			{
				return new RegisterApplicationMasterResponsePBImpl(proxy.RegisterApplicationMaster
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
