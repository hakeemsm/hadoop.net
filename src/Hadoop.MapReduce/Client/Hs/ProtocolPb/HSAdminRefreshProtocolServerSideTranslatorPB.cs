using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.ProtocolPB
{
	public class HSAdminRefreshProtocolServerSideTranslatorPB : HSAdminRefreshProtocolPB
	{
		private readonly HSAdminRefreshProtocol impl;

		private static readonly HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto
			 VoidRefreshAdminAclsResponse = ((HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto
			)HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto.NewBuilder().Build()
			);

		private static readonly HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto
			 VoidRefreshLoadedJobCacheResponse = ((HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto
			)HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto.NewBuilder().Build
			());

		private static readonly HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto
			 VoidRefreshJobRetentionSettingsResponse = ((HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto
			)HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto.NewBuilder
			().Build());

		private static readonly HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto
			 VoidRefreshLogRetentionSettingsResponse = ((HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto
			)HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto.NewBuilder
			().Build());

		public HSAdminRefreshProtocolServerSideTranslatorPB(HSAdminRefreshProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto RefreshAdminAcls
			(RpcController controller, HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto
			 request)
		{
			try
			{
				impl.RefreshAdminAcls();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshAdminAclsResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto RefreshLoadedJobCache
			(RpcController controller, HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto
			 request)
		{
			try
			{
				impl.RefreshLoadedJobCache();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshLoadedJobCacheResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto
			 RefreshJobRetentionSettings(RpcController controller, HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto
			 request)
		{
			try
			{
				impl.RefreshJobRetentionSettings();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshJobRetentionSettingsResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto
			 RefreshLogRetentionSettings(RpcController controller, HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto
			 request)
		{
			try
			{
				impl.RefreshLogRetentionSettings();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshLogRetentionSettingsResponse;
		}
	}
}
