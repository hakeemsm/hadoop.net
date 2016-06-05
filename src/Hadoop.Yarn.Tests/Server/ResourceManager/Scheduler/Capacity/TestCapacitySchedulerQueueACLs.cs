using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestCapacitySchedulerQueueACLs : QueueACLsTestBase
	{
		protected internal override Configuration CreateConfiguration()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { Queuea, Queueb
				 });
			csConf.SetCapacity(CapacitySchedulerConfiguration.Root + "." + Queuea, 50f);
			csConf.SetCapacity(CapacitySchedulerConfiguration.Root + "." + Queueb, 50f);
			IDictionary<QueueACL, AccessControlList> aclsOnQueueA = new Dictionary<QueueACL, 
				AccessControlList>();
			AccessControlList submitACLonQueueA = new AccessControlList(QueueAUser);
			submitACLonQueueA.AddUser(CommonUser);
			AccessControlList adminACLonQueueA = new AccessControlList(QueueAAdmin);
			aclsOnQueueA[QueueACL.SubmitApplications] = submitACLonQueueA;
			aclsOnQueueA[QueueACL.AdministerQueue] = adminACLonQueueA;
			csConf.SetAcls(CapacitySchedulerConfiguration.Root + "." + Queuea, aclsOnQueueA);
			IDictionary<QueueACL, AccessControlList> aclsOnQueueB = new Dictionary<QueueACL, 
				AccessControlList>();
			AccessControlList submitACLonQueueB = new AccessControlList(QueueBUser);
			submitACLonQueueB.AddUser(CommonUser);
			AccessControlList adminACLonQueueB = new AccessControlList(QueueBAdmin);
			aclsOnQueueB[QueueACL.SubmitApplications] = submitACLonQueueB;
			aclsOnQueueB[QueueACL.AdministerQueue] = adminACLonQueueB;
			csConf.SetAcls(CapacitySchedulerConfiguration.Root + "." + Queueb, aclsOnQueueB);
			IDictionary<QueueACL, AccessControlList> aclsOnRootQueue = new Dictionary<QueueACL
				, AccessControlList>();
			AccessControlList submitACLonRoot = new AccessControlList(string.Empty);
			AccessControlList adminACLonRoot = new AccessControlList(RootAdmin);
			aclsOnRootQueue[QueueACL.SubmitApplications] = submitACLonRoot;
			aclsOnRootQueue[QueueACL.AdministerQueue] = adminACLonRoot;
			csConf.SetAcls(CapacitySchedulerConfiguration.Root, aclsOnRootQueue);
			csConf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			csConf.Set("yarn.resourcemanager.scheduler.class", typeof(CapacityScheduler).FullName
				);
			return csConf;
		}
	}
}
