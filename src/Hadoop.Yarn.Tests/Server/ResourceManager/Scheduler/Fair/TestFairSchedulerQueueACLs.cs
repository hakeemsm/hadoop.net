using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairSchedulerQueueACLs : QueueACLsTestBase
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override Configuration CreateConfiguration()
		{
			FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();
			string TestDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp")).GetAbsolutePath
				();
			string AllocFile = new FilePath(TestDir, "test-queues.xml").GetAbsolutePath();
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <aclSubmitApps> </aclSubmitApps>");
			@out.WriteLine("  <aclAdministerApps>root_admin </aclAdministerApps>");
			@out.WriteLine("  <queue name=\"queueA\">");
			@out.WriteLine("    <aclSubmitApps>queueA_user,common_user </aclSubmitApps>");
			@out.WriteLine("    <aclAdministerApps>queueA_admin </aclAdministerApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"queueB\">");
			@out.WriteLine("    <aclSubmitApps>queueB_user,common_user </aclSubmitApps>");
			@out.WriteLine("    <aclAdministerApps>queueB_admin </aclAdministerApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			fsConf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			fsConf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			fsConf.Set("yarn.resourcemanager.scheduler.class", typeof(FairScheduler).FullName
				);
			return fsConf;
		}
	}
}
