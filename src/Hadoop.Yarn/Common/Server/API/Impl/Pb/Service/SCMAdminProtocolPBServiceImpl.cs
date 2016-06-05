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
	public class SCMAdminProtocolPBServiceImpl : SCMAdminProtocolPB
	{
		private SCMAdminProtocol real;

		public SCMAdminProtocolPBServiceImpl(SCMAdminProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto RunCleanerTask
			(RpcController controller, YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto
			 proto)
		{
			RunSharedCacheCleanerTaskRequestPBImpl request = new RunSharedCacheCleanerTaskRequestPBImpl
				(proto);
			try
			{
				RunSharedCacheCleanerTaskResponse response = real.RunCleanerTask(request);
				return ((RunSharedCacheCleanerTaskResponsePBImpl)response).GetProto();
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
