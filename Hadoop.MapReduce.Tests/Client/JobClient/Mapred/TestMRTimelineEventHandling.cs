using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRTimelineEventHandling
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelineServiceStartInMiniCluster()
		{
			Configuration conf = new YarnConfiguration();
			/*
			* Timeline service should not start if the config is set to false
			* Regardless to the value of MAPREDUCE_JOB_EMIT_TIMELINE_DATA
			*/
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, false);
			conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
			MiniMRYarnCluster cluster = null;
			try
			{
				cluster = new MiniMRYarnCluster(typeof(TestJobHistoryEventHandler).Name, 1);
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
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, false);
			conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, false);
			cluster = null;
			try
			{
				cluster = new MiniMRYarnCluster(typeof(TestJobHistoryEventHandler).Name, 1);
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
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRTimelineEventHandling()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
			MiniMRYarnCluster cluster = null;
			try
			{
				cluster = new MiniMRYarnCluster(typeof(TestJobHistoryEventHandler).Name, 1);
				cluster.Init(conf);
				cluster.Start();
				conf.Set(YarnConfiguration.TimelineServiceWebappAddress, MiniYARNCluster.GetHostname
					() + ":" + cluster.GetApplicationHistoryServer().GetPort());
				TimelineStore ts = cluster.GetApplicationHistoryServer().GetTimelineStore();
				Path inDir = new Path("input");
				Path outDir = new Path("output");
				RunningJob job = UtilsForTests.RunJobSucceed(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Succeeded, job.GetJobStatus().GetState(
					).GetValue());
				TimelineEntities entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null
					, null, null, null, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				TimelineEntity tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(job.GetID().ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual("MAPREDUCE_JOB", tEntity.GetEntityType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[tEntity.GetEvents().Count - 1].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobFinished.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				job = UtilsForTests.RunJobFail(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Failed, job.GetJobStatus().GetState().GetValue
					());
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(2, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(job.GetID().ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual("MAPREDUCE_JOB", tEntity.GetEntityType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[tEntity.GetEvents().Count - 1].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobFailed.ToString(), tEntity.GetEvents
					()[0].GetEventType());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapreduceJobTimelineServiceEnabled()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, false);
			MiniMRYarnCluster cluster = null;
			try
			{
				cluster = new MiniMRYarnCluster(typeof(TestJobHistoryEventHandler).Name, 1);
				cluster.Init(conf);
				cluster.Start();
				conf.Set(YarnConfiguration.TimelineServiceWebappAddress, MiniYARNCluster.GetHostname
					() + ":" + cluster.GetApplicationHistoryServer().GetPort());
				TimelineStore ts = cluster.GetApplicationHistoryServer().GetTimelineStore();
				Path inDir = new Path("input");
				Path outDir = new Path("output");
				RunningJob job = UtilsForTests.RunJobSucceed(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Succeeded, job.GetJobStatus().GetState(
					).GetValue());
				TimelineEntities entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null
					, null, null, null, null, null);
				NUnit.Framework.Assert.AreEqual(0, entities.GetEntities().Count);
				conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
				job = UtilsForTests.RunJobSucceed(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Succeeded, job.GetJobStatus().GetState(
					).GetValue());
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				TimelineEntity tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(job.GetID().ToString(), tEntity.GetEntityId());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Stop();
				}
			}
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
			cluster = null;
			try
			{
				cluster = new MiniMRYarnCluster(typeof(TestJobHistoryEventHandler).Name, 1);
				cluster.Init(conf);
				cluster.Start();
				conf.Set(YarnConfiguration.TimelineServiceWebappAddress, MiniYARNCluster.GetHostname
					() + ":" + cluster.GetApplicationHistoryServer().GetPort());
				TimelineStore ts = cluster.GetApplicationHistoryServer().GetTimelineStore();
				Path inDir = new Path("input");
				Path outDir = new Path("output");
				conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, false);
				RunningJob job = UtilsForTests.RunJobSucceed(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Succeeded, job.GetJobStatus().GetState(
					).GetValue());
				TimelineEntities entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null
					, null, null, null, null, null);
				NUnit.Framework.Assert.AreEqual(0, entities.GetEntities().Count);
				conf.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
				job = UtilsForTests.RunJobSucceed(new JobConf(conf), inDir, outDir);
				NUnit.Framework.Assert.AreEqual(JobStatus.Succeeded, job.GetJobStatus().GetState(
					).GetValue());
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				TimelineEntity tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(job.GetID().ToString(), tEntity.GetEntityId());
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
