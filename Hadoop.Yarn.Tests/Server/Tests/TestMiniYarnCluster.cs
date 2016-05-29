using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class TestMiniYarnCluster
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelineServiceStartInMiniCluster()
		{
			Configuration conf = new YarnConfiguration();
			int numNodeManagers = 1;
			int numLocalDirs = 1;
			int numLogDirs = 1;
			bool enableAHS;
			/*
			* Timeline service should not start if TIMELINE_SERVICE_ENABLED == false
			* and enableAHS flag == false
			*/
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, false);
			enableAHS = false;
			MiniYARNCluster cluster = null;
			try
			{
				cluster = new MiniYARNCluster(typeof(TestMiniYarnCluster).Name, numNodeManagers, 
					numLocalDirs, numLogDirs, numLogDirs, enableAHS);
				cluster.Init(conf);
				cluster.Start();
				//verify that the timeline service is not started.
				NUnit.Framework.Assert.IsNull("Timeline Service should not have been started", cluster
					.GetApplicationHistoryServer());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Stop();
				}
			}
			/*
			* Timeline service should start if TIMELINE_SERVICE_ENABLED == true
			* and enableAHS == false
			*/
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			enableAHS = false;
			cluster = null;
			try
			{
				cluster = new MiniYARNCluster(typeof(TestMiniYarnCluster).Name, numNodeManagers, 
					numLocalDirs, numLogDirs, numLogDirs, enableAHS);
				cluster.Init(conf);
				// Verify that the timeline-service starts on ephemeral ports by default
				string hostname = MiniYARNCluster.GetHostname();
				NUnit.Framework.Assert.AreEqual(hostname + ":0", conf.Get(YarnConfiguration.TimelineServiceAddress
					));
				NUnit.Framework.Assert.AreEqual(hostname + ":0", conf.Get(YarnConfiguration.TimelineServiceWebappAddress
					));
				cluster.Start();
				//Timeline service may sometime take a while to get started
				int wait = 0;
				while (cluster.GetApplicationHistoryServer() == null && wait < 20)
				{
					Sharpen.Thread.Sleep(500);
					wait++;
				}
				//verify that the timeline service is started.
				NUnit.Framework.Assert.IsNotNull("Timeline Service should have been started", cluster
					.GetApplicationHistoryServer());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Stop();
				}
			}
			/*
			* Timeline service should start if TIMELINE_SERVICE_ENABLED == false
			* and enableAHS == true
			*/
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, false);
			enableAHS = true;
			cluster = null;
			try
			{
				cluster = new MiniYARNCluster(typeof(TestMiniYarnCluster).Name, numNodeManagers, 
					numLocalDirs, numLogDirs, numLogDirs, enableAHS);
				cluster.Init(conf);
				cluster.Start();
				//Timeline service may sometime take a while to get started
				int wait = 0;
				while (cluster.GetApplicationHistoryServer() == null && wait < 20)
				{
					Sharpen.Thread.Sleep(500);
					wait++;
				}
				//verify that the timeline service is started.
				NUnit.Framework.Assert.IsNotNull("Timeline Service should have been started", cluster
					.GetApplicationHistoryServer());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Stop();
				}
			}
		}
	}
}
