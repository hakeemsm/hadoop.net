using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Impl.PB.Service
{
	public class SCMUploaderProtocolPBServiceImpl : SCMUploaderProtocolPB
	{
		private SCMUploaderProtocol real;

		public SCMUploaderProtocolPBServiceImpl(SCMUploaderProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto Notify
			(RpcController controller, YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto
			 proto)
		{
			SCMUploaderNotifyRequestPBImpl request = new SCMUploaderNotifyRequestPBImpl(proto
				);
			try
			{
				SCMUploaderNotifyResponse response = real.Notify(request);
				return ((SCMUploaderNotifyResponsePBImpl)response).GetProto();
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
		public virtual YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto CanUpload
			(RpcController controller, YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto
			 proto)
		{
			SCMUploaderCanUploadRequestPBImpl request = new SCMUploaderCanUploadRequestPBImpl
				(proto);
			try
			{
				SCMUploaderCanUploadResponse response = real.CanUpload(request);
				return ((SCMUploaderCanUploadResponsePBImpl)response).GetProto();
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
