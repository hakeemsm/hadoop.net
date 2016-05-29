using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>
	/// This test replicates the condition os MAPREDUCE-3431 -a failure
	/// during startup triggered an NPE during shutdown
	/// </summary>
	public class TestDelegationTokenRenewerLifecycle
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupFailure()
		{
			Configuration conf = new Configuration();
			DelegationTokenRenewer delegationTokenRenewer = new DelegationTokenRenewer();
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			delegationTokenRenewer.SetRMContext(mockContext);
			delegationTokenRenewer.Init(conf);
			delegationTokenRenewer.Stop();
		}
	}
}
