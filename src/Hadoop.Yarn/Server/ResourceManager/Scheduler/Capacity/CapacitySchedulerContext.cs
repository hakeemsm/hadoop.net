using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	/// <summary>
	/// Read-only interface to
	/// <see cref="CapacityScheduler"/>
	/// context.
	/// </summary>
	public interface CapacitySchedulerContext
	{
		CapacitySchedulerConfiguration GetConfiguration();

		Resource GetMinimumResourceCapability();

		Resource GetMaximumResourceCapability();

		Resource GetMaximumResourceCapability(string queueName);

		RMContainerTokenSecretManager GetContainerTokenSecretManager();

		int GetNumClusterNodes();

		RMContext GetRMContext();

		Resource GetClusterResource();

		/// <summary>Get the yarn configuration.</summary>
		Configuration GetConf();

		IComparer<FiCaSchedulerApp> GetApplicationComparator();

		ResourceCalculator GetResourceCalculator();

		IComparer<CSQueue> GetQueueComparator();

		FiCaSchedulerNode GetNode(NodeId nodeId);
	}
}
