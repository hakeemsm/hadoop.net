using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor
{
	public class TestSchedulingMonitor
	{
		public virtual void TestRMStarts()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmSchedulerEnableMonitors, true);
			conf.Set(YarnConfiguration.RmSchedulerMonitorPolicies, typeof(ProportionalCapacityPreemptionPolicy
				).GetCanonicalName());
			ResourceManager rm = new ResourceManager();
			try
			{
				rm.Init(conf);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("ResourceManager does not start when " + YarnConfiguration
					.RmSchedulerEnableMonitors + " is set to true");
			}
		}
	}
}
