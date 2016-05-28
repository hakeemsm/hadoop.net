using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tracing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for HDFS protocols.
	/// </summary>
	public class HDFSPolicyProvider : PolicyProvider
	{
		private static readonly Service[] hdfsServices = new Service[] { new Service(CommonConfigurationKeys
			.SecurityClientProtocolAcl, typeof(ClientProtocol)), new Service(CommonConfigurationKeys
			.SecurityClientDatanodeProtocolAcl, typeof(ClientDatanodeProtocol)), new Service
			(CommonConfigurationKeys.SecurityDatanodeProtocolAcl, typeof(DatanodeProtocol)), 
			new Service(CommonConfigurationKeys.SecurityInterDatanodeProtocolAcl, typeof(InterDatanodeProtocol
			)), new Service(CommonConfigurationKeys.SecurityNamenodeProtocolAcl, typeof(NamenodeProtocol
			)), new Service(CommonConfigurationKeys.SecurityQjournalServiceProtocolAcl, typeof(
			QJournalProtocol)), new Service(CommonConfigurationKeys.SecurityHaServiceProtocolAcl
			, typeof(HAServiceProtocol)), new Service(CommonConfigurationKeys.SecurityZkfcProtocolAcl
			, typeof(ZKFCProtocol)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationRefreshPolicy
			, typeof(RefreshAuthorizationPolicyProtocol)), new Service(CommonConfigurationKeys
			.HadoopSecurityServiceAuthorizationRefreshUserMappings, typeof(RefreshUserMappingsProtocol
			)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationGetUserMappings
			, typeof(GetUserMappingsProtocol)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationRefreshCallqueue
			, typeof(RefreshCallQueueProtocol)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationGenericRefresh
			, typeof(GenericRefreshProtocol)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationTracing
			, typeof(TraceAdminProtocol)) };

		public override Service[] GetServices()
		{
			return hdfsServices;
		}
	}
}
