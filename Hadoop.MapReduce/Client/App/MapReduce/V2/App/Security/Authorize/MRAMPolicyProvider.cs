using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for YARN MapReduce protocols.
	/// </summary>
	public class MRAMPolicyProvider : PolicyProvider
	{
		private static readonly Service[] mapReduceApplicationMasterServices = new Service
			[] { new Service(MRJobConfig.MrAmSecurityServiceAuthorizationTaskUmbilical, typeof(
			TaskUmbilicalProtocol)), new Service(MRJobConfig.MrAmSecurityServiceAuthorizationClient
			, typeof(MRClientProtocolPB)) };

		public override Service[] GetServices()
		{
			return mapReduceApplicationMasterServices;
		}
	}
}
