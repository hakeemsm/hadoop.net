using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Service
{
	public class ApplicationMasterProtocolPBServiceImpl : ApplicationMasterProtocolPB
	{
		private ApplicationMasterProtocol real;

		public ApplicationMasterProtocolPBServiceImpl(ApplicationMasterProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.AllocateResponseProto Allocate(RpcController arg0
			, YarnServiceProtos.AllocateRequestProto proto)
		{
			AllocateRequestPBImpl request = new AllocateRequestPBImpl(proto);
			try
			{
				AllocateResponse response = real.Allocate(request);
				return ((AllocateResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.FinishApplicationMasterResponseProto FinishApplicationMaster
			(RpcController arg0, YarnServiceProtos.FinishApplicationMasterRequestProto proto
			)
		{
			FinishApplicationMasterRequestPBImpl request = new FinishApplicationMasterRequestPBImpl
				(proto);
			try
			{
				FinishApplicationMasterResponse response = real.FinishApplicationMaster(request);
				return ((FinishApplicationMasterResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.RegisterApplicationMasterResponseProto RegisterApplicationMaster
			(RpcController arg0, YarnServiceProtos.RegisterApplicationMasterRequestProto proto
			)
		{
			RegisterApplicationMasterRequestPBImpl request = new RegisterApplicationMasterRequestPBImpl
				(proto);
			try
			{
				RegisterApplicationMasterResponse response = real.RegisterApplicationMaster(request
					);
				return ((RegisterApplicationMasterResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
