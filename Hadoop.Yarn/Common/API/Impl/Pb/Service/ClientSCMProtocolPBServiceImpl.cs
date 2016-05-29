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
	public class ClientSCMProtocolPBServiceImpl : ClientSCMProtocolPB
	{
		private ClientSCMProtocol real;

		public ClientSCMProtocolPBServiceImpl(ClientSCMProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.UseSharedCacheResourceResponseProto Use(RpcController
			 controller, YarnServiceProtos.UseSharedCacheResourceRequestProto proto)
		{
			UseSharedCacheResourceRequestPBImpl request = new UseSharedCacheResourceRequestPBImpl
				(proto);
			try
			{
				UseSharedCacheResourceResponse response = real.Use(request);
				return ((UseSharedCacheResourceResponsePBImpl)response).GetProto();
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
		public virtual YarnServiceProtos.ReleaseSharedCacheResourceResponseProto Release(
			RpcController controller, YarnServiceProtos.ReleaseSharedCacheResourceRequestProto
			 proto)
		{
			ReleaseSharedCacheResourceRequestPBImpl request = new ReleaseSharedCacheResourceRequestPBImpl
				(proto);
			try
			{
				ReleaseSharedCacheResourceResponse response = real.Release(request);
				return ((ReleaseSharedCacheResourceResponsePBImpl)response).GetProto();
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
