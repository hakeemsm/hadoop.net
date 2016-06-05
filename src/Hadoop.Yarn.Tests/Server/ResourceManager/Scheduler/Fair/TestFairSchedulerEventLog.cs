using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairSchedulerEventLog
	{
		private FilePath logFile;

		private FairScheduler scheduler;

		private ResourceManager resourceManager;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			scheduler = new FairScheduler();
			Configuration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(ResourceScheduler
				));
			conf.Set("yarn.scheduler.fair.event-log-enabled", "true");
			// All tests assume only one assignment per node update
			conf.Set(FairSchedulerConfiguration.AssignMultiple, "false");
			resourceManager = new ResourceManager();
			resourceManager.Init(conf);
			((AsyncDispatcher)resourceManager.GetRMContext().GetDispatcher()).Start();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
		}

		/// <summary>Make sure the scheduler creates the event log.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateEventLog()
		{
			FairSchedulerEventLog eventLog = scheduler.GetEventLog();
			logFile = new FilePath(eventLog.GetLogFile());
			NUnit.Framework.Assert.IsTrue(logFile.Exists());
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			logFile.Delete();
			logFile.GetParentFile().Delete();
			// fairscheduler/
			if (scheduler != null)
			{
				scheduler.Stop();
				scheduler = null;
			}
			if (resourceManager != null)
			{
				resourceManager.Stop();
				resourceManager = null;
			}
		}
	}
}
