using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class TestJobHistoryEventHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJobHistoryEventHandler
			));

		private static MiniDFSCluster dfsCluster = null;

		private static string coreSitePath;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpClass()
		{
			coreSitePath = "." + FilePath.separator + "target" + FilePath.separator + "test-classes"
				 + FilePath.separator + "core-site.xml";
			Configuration conf = new HdfsConfiguration();
			dfsCluster = new MiniDFSCluster.Builder(conf).Build();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void CleanUpClass()
		{
			dfsCluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanTest()
		{
			new FilePath(coreSitePath).Delete();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFirstFlushOnCompletionEvent()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, t.workDir);
			conf.SetLong(MRJobConfig.MrAmHistoryCompleteEventFlushTimeoutMs, 60 * 1000l);
			conf.SetInt(MRJobConfig.MrAmHistoryJobCompleteUnflushedMultiplier, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryMaxUnflushedCompleteEvents, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryUseBatchedFlushQueueSizeThreshold, 200);
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter).Write(Matchers.Any<HistoryEvent>());
				for (int i = 0; i < 100; i++)
				{
					QueueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskStartedEvent(t.taskID, 0, TaskType
						.Map, string.Empty)));
				}
				HandleNextNEvents(jheh, 100);
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(0)).Flush();
				// First completion event, but min-queue-size for batching flushes is 10
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(t.taskID, t.
					taskAttemptID, 0, TaskType.Map, string.Empty, null)));
				Org.Mockito.Mockito.Verify(mockWriter).Flush();
			}
			finally
			{
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxUnflushedCompletionEvents()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, t.workDir);
			conf.SetLong(MRJobConfig.MrAmHistoryCompleteEventFlushTimeoutMs, 60 * 1000l);
			conf.SetInt(MRJobConfig.MrAmHistoryJobCompleteUnflushedMultiplier, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryMaxUnflushedCompleteEvents, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryUseBatchedFlushQueueSizeThreshold, 5);
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter).Write(Matchers.Any<HistoryEvent>());
				for (int i = 0; i < 100; i++)
				{
					QueueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(t.taskID, t.taskAttemptID
						, 0, TaskType.Map, string.Empty, null)));
				}
				HandleNextNEvents(jheh, 9);
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(0)).Flush();
				HandleNextNEvents(jheh, 1);
				Org.Mockito.Mockito.Verify(mockWriter).Flush();
				HandleNextNEvents(jheh, 50);
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(6)).Flush();
			}
			finally
			{
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnflushedTimer()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, t.workDir);
			conf.SetLong(MRJobConfig.MrAmHistoryCompleteEventFlushTimeoutMs, 2 * 1000l);
			//2 seconds.
			conf.SetInt(MRJobConfig.MrAmHistoryJobCompleteUnflushedMultiplier, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryMaxUnflushedCompleteEvents, 100);
			conf.SetInt(MRJobConfig.MrAmHistoryUseBatchedFlushQueueSizeThreshold, 5);
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter).Write(Matchers.Any<HistoryEvent>());
				for (int i = 0; i < 100; i++)
				{
					QueueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(t.taskID, t.taskAttemptID
						, 0, TaskType.Map, string.Empty, null)));
				}
				HandleNextNEvents(jheh, 9);
				NUnit.Framework.Assert.IsTrue(jheh.GetFlushTimerStatus());
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(0)).Flush();
				Sharpen.Thread.Sleep(2 * 4 * 1000l);
				// 4 seconds should be enough. Just be safe.
				Org.Mockito.Mockito.Verify(mockWriter).Flush();
				NUnit.Framework.Assert.IsFalse(jheh.GetFlushTimerStatus());
			}
			finally
			{
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBatchedFlushJobEndMultiplier()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, t.workDir);
			conf.SetLong(MRJobConfig.MrAmHistoryCompleteEventFlushTimeoutMs, 60 * 1000l);
			//2 seconds.
			conf.SetInt(MRJobConfig.MrAmHistoryJobCompleteUnflushedMultiplier, 3);
			conf.SetInt(MRJobConfig.MrAmHistoryMaxUnflushedCompleteEvents, 10);
			conf.SetInt(MRJobConfig.MrAmHistoryUseBatchedFlushQueueSizeThreshold, 0);
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter).Write(Matchers.Any<HistoryEvent>());
				for (int i = 0; i < 100; i++)
				{
					QueueEvent(jheh, new JobHistoryEvent(t.jobId, new TaskFinishedEvent(t.taskID, t.taskAttemptID
						, 0, TaskType.Map, string.Empty, null)));
				}
				QueueEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(TypeConverter.
					FromYarn(t.jobId), 0, 10, 10, 0, 0, null, null, new Counters())));
				HandleNextNEvents(jheh, 29);
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(0)).Flush();
				HandleNextNEvents(jheh, 72);
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(4)).Flush();
			}
			finally
			{
				//3 * 30 + 1 for JobFinished
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		// In case of all types of events, process Done files if it's last AM retry
		/// <exception cref="System.Exception"/>
		public virtual void TestProcessDoneFilesOnLastAMRetry()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this, true);
			Configuration conf = new Configuration();
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(0)).ProcessDoneFiles(Matchers.Any
					<JobId>());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Error.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(1)).ProcessDoneFiles(Matchers.Any
					<JobId>());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(TypeConverter
					.FromYarn(t.jobId), 0, 0, 0, 0, 0, new Counters(), new Counters(), new Counters(
					))));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(2)).ProcessDoneFiles(Matchers.Any
					<JobId>());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Failed.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(3)).ProcessDoneFiles(Matchers.Any
					<JobId>());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Killed.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(4)).ProcessDoneFiles(Matchers.Any
					<JobId>());
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(5)).Write(Matchers.Any
					<HistoryEvent>());
			}
			finally
			{
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		// Skip processing Done files in case of ERROR, if it's not last AM retry
		/// <exception cref="System.Exception"/>
		public virtual void TestProcessDoneFilesNotLastAMRetry()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this, false);
			Configuration conf = new Configuration();
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(0)).ProcessDoneFiles(t
					.jobId);
				// skip processing done files
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Error.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(0)).ProcessDoneFiles(t
					.jobId);
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(TypeConverter
					.FromYarn(t.jobId), 0, 0, 0, 0, 0, new Counters(), new Counters(), new Counters(
					))));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(1)).ProcessDoneFiles(t
					.jobId);
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Failed.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(2)).ProcessDoneFiles(t
					.jobId);
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Killed.ToString())));
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(3)).ProcessDoneFiles(t
					.jobId);
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(5)).Write(Matchers.Any
					<HistoryEvent>());
			}
			finally
			{
				jheh.Stop();
				Org.Mockito.Mockito.Verify(mockWriter).Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDefaultFsIsUsedForHistory()
		{
			// Create default configuration pointing to the minicluster
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, dfsCluster.GetURI().ToString
				());
			FileOutputStream os = new FileOutputStream(coreSitePath);
			conf.WriteXml(os);
			os.Close();
			// simulate execution under a non-default namenode
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "file:///");
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			conf.Set(MRJobConfig.MrAmStagingDir, t.dfsWorkDir);
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0, false
				);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1)));
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(TypeConverter
					.FromYarn(t.jobId), 0, 0, 0, 0, 0, new Counters(), new Counters(), new Counters(
					))));
				// If we got here then event handler worked but we don't know with which
				// file system. Now we check that history stuff was written to minicluster
				FileSystem dfsFileSystem = dfsCluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Minicluster contains some history files", dfsFileSystem
					.GlobStatus(new Path(t.dfsWorkDir + "/*")).Length != 0);
				FileSystem localFileSystem = LocalFileSystem.Get(conf);
				NUnit.Framework.Assert.IsFalse("No history directory on non-default file system", 
					localFileSystem.Exists(new Path(t.dfsWorkDir)));
			}
			finally
			{
				jheh.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetHistoryIntermediateDoneDirForUser()
		{
			// Test relative path
			Configuration conf = new Configuration();
			conf.Set(JHAdminConfig.MrHistoryIntermediateDoneDir, "/mapred/history/done_intermediate"
				);
			conf.Set(MRJobConfig.UserName, Runtime.GetProperty("user.name"));
			string pathStr = JobHistoryUtils.GetHistoryIntermediateDoneDirForUser(conf);
			NUnit.Framework.Assert.AreEqual("/mapred/history/done_intermediate/" + Runtime.GetProperty
				("user.name"), pathStr);
			// Test fully qualified path
			// Create default configuration pointing to the minicluster
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, dfsCluster.GetURI().ToString
				());
			FileOutputStream os = new FileOutputStream(coreSitePath);
			conf.WriteXml(os);
			os.Close();
			// Simulate execution under a non-default namenode
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "file:///");
			pathStr = JobHistoryUtils.GetHistoryIntermediateDoneDirForUser(conf);
			NUnit.Framework.Assert.AreEqual(dfsCluster.GetURI().ToString() + "/mapred/history/done_intermediate/"
				 + Runtime.GetProperty("user.name"), pathStr);
		}

		// test AMStartedEvent for submitTime and startTime
		/// <exception cref="System.Exception"/>
		public virtual void TestAMStartedEvent()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this);
			Configuration conf = new Configuration();
			JHEvenHandlerForTest realJheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
			JHEvenHandlerForTest jheh = Org.Mockito.Mockito.Spy(realJheh);
			jheh.Init(conf);
			EventWriter mockWriter = null;
			try
			{
				jheh.Start();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, 100)));
				JobHistoryEventHandler.MetaInfo mi = JobHistoryEventHandler.fileMap[t.jobId];
				NUnit.Framework.Assert.AreEqual(mi.GetJobIndexInfo().GetSubmitTime(), 100);
				NUnit.Framework.Assert.AreEqual(mi.GetJobIndexInfo().GetJobStartTime(), 200);
				NUnit.Framework.Assert.AreEqual(mi.GetJobSummary().GetJobSubmitTime(), 100);
				NUnit.Framework.Assert.AreEqual(mi.GetJobSummary().GetJobLaunchTime(), 200);
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Failed.ToString())));
				NUnit.Framework.Assert.AreEqual(mi.GetJobIndexInfo().GetSubmitTime(), 100);
				NUnit.Framework.Assert.AreEqual(mi.GetJobIndexInfo().GetJobStartTime(), 200);
				NUnit.Framework.Assert.AreEqual(mi.GetJobSummary().GetJobSubmitTime(), 100);
				NUnit.Framework.Assert.AreEqual(mi.GetJobSummary().GetJobLaunchTime(), 200);
				Org.Mockito.Mockito.Verify(jheh, Org.Mockito.Mockito.Times(1)).ProcessDoneFiles(t
					.jobId);
				mockWriter = jheh.GetEventWriter();
				Org.Mockito.Mockito.Verify(mockWriter, Org.Mockito.Mockito.Times(2)).Write(Matchers.Any
					<HistoryEvent>());
			}
			finally
			{
				jheh.Stop();
			}
		}

		// Have JobHistoryEventHandler handle some events and make sure they get
		// stored to the Timeline store
		/// <exception cref="System.Exception"/>
		public virtual void TestTimelineEventHandling()
		{
			TestJobHistoryEventHandler.TestParams t = new TestJobHistoryEventHandler.TestParams
				(this, false);
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			MiniYARNCluster yarnCluster = null;
			long currentTime = Runtime.CurrentTimeMillis();
			try
			{
				yarnCluster = new MiniYARNCluster(typeof(TestJobHistoryEventHandler).Name, 1, 1, 
					1, 1);
				yarnCluster.Init(conf);
				yarnCluster.Start();
				Configuration confJHEH = new YarnConfiguration(conf);
				confJHEH.SetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, true);
				confJHEH.Set(YarnConfiguration.TimelineServiceWebappAddress, MiniYARNCluster.GetHostname
					() + ":" + yarnCluster.GetApplicationHistoryServer().GetPort());
				JHEvenHandlerForTest jheh = new JHEvenHandlerForTest(t.mockAppContext, 0);
				jheh.Init(confJHEH);
				jheh.Start();
				TimelineStore ts = yarnCluster.GetApplicationHistoryServer().GetTimelineStore();
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(t.appAttemptId, 
					200, t.containerId, "nmhost", 3000, 4000, -1), currentTime - 10));
				TimelineEntities entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null
					, null, null, null, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				TimelineEntity tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.jobId.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(1, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(currentTime - 10, tEntity.GetEvents()[0].GetTimestamp
					());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobSubmittedEvent(TypeConverter
					.FromYarn(t.jobId), "name", "user", 200, "/foo/job.xml", new Dictionary<JobACL, 
					AccessControlList>(), "default"), currentTime + 10));
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.jobId.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(2, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.JobSubmitted.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[1].GetEventType());
				NUnit.Framework.Assert.AreEqual(currentTime + 10, tEntity.GetEvents()[0].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime - 10, tEntity.GetEvents()[1].GetTimestamp
					());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobQueueChangeEvent(TypeConverter
					.FromYarn(t.jobId), "q2"), currentTime - 20));
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.jobId.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(3, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.JobSubmitted.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[1].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobQueueChanged.ToString(), tEntity.GetEvents
					()[2].GetEventType());
				NUnit.Framework.Assert.AreEqual(currentTime + 10, tEntity.GetEvents()[0].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime - 10, tEntity.GetEvents()[1].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime - 20, tEntity.GetEvents()[2].GetTimestamp
					());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(TypeConverter
					.FromYarn(t.jobId), 0, 0, 0, 0, 0, new Counters(), new Counters(), new Counters(
					)), currentTime));
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.jobId.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(4, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.JobSubmitted.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobFinished.ToString(), tEntity.GetEvents
					()[1].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[2].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobQueueChanged.ToString(), tEntity.GetEvents
					()[3].GetEventType());
				NUnit.Framework.Assert.AreEqual(currentTime + 10, tEntity.GetEvents()[0].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime, tEntity.GetEvents()[1].GetTimestamp(
					));
				NUnit.Framework.Assert.AreEqual(currentTime - 10, tEntity.GetEvents()[2].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime - 20, tEntity.GetEvents()[3].GetTimestamp
					());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new JobUnsuccessfulCompletionEvent
					(TypeConverter.FromYarn(t.jobId), 0, 0, 0, JobStateInternal.Killed.ToString()), 
					currentTime + 20));
				entities = ts.GetEntities("MAPREDUCE_JOB", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.jobId.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(5, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.JobKilled.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobSubmitted.ToString(), tEntity.GetEvents
					()[1].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobFinished.ToString(), tEntity.GetEvents
					()[2].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.AmStarted.ToString(), tEntity.GetEvents
					()[3].GetEventType());
				NUnit.Framework.Assert.AreEqual(EventType.JobQueueChanged.ToString(), tEntity.GetEvents
					()[4].GetEventType());
				NUnit.Framework.Assert.AreEqual(currentTime + 20, tEntity.GetEvents()[0].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime + 10, tEntity.GetEvents()[1].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime, tEntity.GetEvents()[2].GetTimestamp(
					));
				NUnit.Framework.Assert.AreEqual(currentTime - 10, tEntity.GetEvents()[3].GetTimestamp
					());
				NUnit.Framework.Assert.AreEqual(currentTime - 20, tEntity.GetEvents()[4].GetTimestamp
					());
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new TaskStartedEvent(t.taskID, 0, 
					TaskType.Map, string.Empty)));
				entities = ts.GetEntities("MAPREDUCE_TASK", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.taskID.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(1, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.TaskStarted.ToString(), tEntity.GetEvents
					()[0].GetEventType());
				NUnit.Framework.Assert.AreEqual(TaskType.Map.ToString(), tEntity.GetEvents()[0].GetEventInfo
					()["TASK_TYPE"]);
				HandleEvent(jheh, new JobHistoryEvent(t.jobId, new TaskStartedEvent(t.taskID, 0, 
					TaskType.Reduce, string.Empty)));
				entities = ts.GetEntities("MAPREDUCE_TASK", null, null, null, null, null, null, null
					, null, null);
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				tEntity = entities.GetEntities()[0];
				NUnit.Framework.Assert.AreEqual(t.taskID.ToString(), tEntity.GetEntityId());
				NUnit.Framework.Assert.AreEqual(2, tEntity.GetEvents().Count);
				NUnit.Framework.Assert.AreEqual(EventType.TaskStarted.ToString(), tEntity.GetEvents
					()[1].GetEventType());
				NUnit.Framework.Assert.AreEqual(TaskType.Reduce.ToString(), tEntity.GetEvents()[0
					].GetEventInfo()["TASK_TYPE"]);
				NUnit.Framework.Assert.AreEqual(TaskType.Map.ToString(), tEntity.GetEvents()[1].GetEventInfo
					()["TASK_TYPE"]);
			}
			finally
			{
				if (yarnCluster != null)
				{
					yarnCluster.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCountersToJSON()
		{
			JobHistoryEventHandler jheh = new JobHistoryEventHandler(null, 0);
			Counters counters = new Counters();
			CounterGroup group1 = counters.AddGroup("DOCTORS", "Incarnations of the Doctor");
			group1.AddCounter("PETER_CAPALDI", "Peter Capaldi", 12);
			group1.AddCounter("MATT_SMITH", "Matt Smith", 11);
			group1.AddCounter("DAVID_TENNANT", "David Tennant", 10);
			CounterGroup group2 = counters.AddGroup("COMPANIONS", "Companions of the Doctor");
			group2.AddCounter("CLARA_OSWALD", "Clara Oswald", 6);
			group2.AddCounter("RORY_WILLIAMS", "Rory Williams", 5);
			group2.AddCounter("AMY_POND", "Amy Pond", 4);
			group2.AddCounter("MARTHA_JONES", "Martha Jones", 3);
			group2.AddCounter("DONNA_NOBLE", "Donna Noble", 2);
			group2.AddCounter("ROSE_TYLER", "Rose Tyler", 1);
			JsonNode jsonNode = jheh.CountersToJSON(counters);
			string jsonStr = new ObjectMapper().WriteValueAsString(jsonNode);
			string expected = "[{\"NAME\":\"COMPANIONS\",\"DISPLAY_NAME\":\"Companions " + "of the Doctor\",\"COUNTERS\":[{\"NAME\":\"AMY_POND\",\"DISPLAY_NAME\""
				 + ":\"Amy Pond\",\"VALUE\":4},{\"NAME\":\"CLARA_OSWALD\"," + "\"DISPLAY_NAME\":\"Clara Oswald\",\"VALUE\":6},{\"NAME\":"
				 + "\"DONNA_NOBLE\",\"DISPLAY_NAME\":\"Donna Noble\",\"VALUE\":2}," + "{\"NAME\":\"MARTHA_JONES\",\"DISPLAY_NAME\":\"Martha Jones\","
				 + "\"VALUE\":3},{\"NAME\":\"RORY_WILLIAMS\",\"DISPLAY_NAME\":\"Rory " + "Williams\",\"VALUE\":5},{\"NAME\":\"ROSE_TYLER\",\"DISPLAY_NAME\":"
				 + "\"Rose Tyler\",\"VALUE\":1}]},{\"NAME\":\"DOCTORS\",\"DISPLAY_NAME\"" + ":\"Incarnations of the Doctor\",\"COUNTERS\":[{\"NAME\":"
				 + "\"DAVID_TENNANT\",\"DISPLAY_NAME\":\"David Tennant\",\"VALUE\":10}," + "{\"NAME\":\"MATT_SMITH\",\"DISPLAY_NAME\":\"Matt Smith\",\"VALUE\":"
				 + "11},{\"NAME\":\"PETER_CAPALDI\",\"DISPLAY_NAME\":\"Peter Capaldi\"," + "\"VALUE\":12}]}]";
			NUnit.Framework.Assert.AreEqual(expected, jsonStr);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCountersToJSONEmpty()
		{
			JobHistoryEventHandler jheh = new JobHistoryEventHandler(null, 0);
			Counters counters = null;
			JsonNode jsonNode = jheh.CountersToJSON(counters);
			string jsonStr = new ObjectMapper().WriteValueAsString(jsonNode);
			string expected = "[]";
			NUnit.Framework.Assert.AreEqual(expected, jsonStr);
			counters = new Counters();
			jsonNode = jheh.CountersToJSON(counters);
			jsonStr = new ObjectMapper().WriteValueAsString(jsonNode);
			expected = "[]";
			NUnit.Framework.Assert.AreEqual(expected, jsonStr);
			counters.AddGroup("DOCTORS", "Incarnations of the Doctor");
			jsonNode = jheh.CountersToJSON(counters);
			jsonStr = new ObjectMapper().WriteValueAsString(jsonNode);
			expected = "[{\"NAME\":\"DOCTORS\",\"DISPLAY_NAME\":\"Incarnations of the " + "Doctor\",\"COUNTERS\":[]}]";
			NUnit.Framework.Assert.AreEqual(expected, jsonStr);
		}

		private void QueueEvent(JHEvenHandlerForTest jheh, JobHistoryEvent @event)
		{
			jheh.Handle(@event);
		}

		/// <exception cref="System.Exception"/>
		private void HandleEvent(JHEvenHandlerForTest jheh, JobHistoryEvent @event)
		{
			jheh.Handle(@event);
			jheh.HandleEvent(jheh.eventQueue.Take());
		}

		/// <exception cref="System.Exception"/>
		private void HandleNextNEvents(JHEvenHandlerForTest jheh, int numEvents)
		{
			for (int i = 0; i < numEvents; i++)
			{
				jheh.HandleEvent(jheh.eventQueue.Take());
			}
		}

		private string SetupTestWorkDir()
		{
			FilePath testWorkDir = new FilePath("target", this.GetType().GetCanonicalName());
			try
			{
				FileContext.GetLocalFSFileContext().Delete(new Path(testWorkDir.GetAbsolutePath()
					), true);
				return testWorkDir.GetAbsolutePath();
			}
			catch (Exception e)
			{
				Log.Warn("Could not cleanup", e);
				throw new YarnRuntimeException("could not cleanup test dir", e);
			}
		}

		private AppContext MockAppContext(ApplicationId appId, bool isLastAMRetry)
		{
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(appId));
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetAllCounters()).ThenReturn(new Counters());
			Org.Mockito.Mockito.When(mockJob.GetTotalMaps()).ThenReturn(10);
			Org.Mockito.Mockito.When(mockJob.GetTotalReduces()).ThenReturn(10);
			Org.Mockito.Mockito.When(mockJob.GetName()).ThenReturn("mockjob");
			Org.Mockito.Mockito.When(mockContext.GetJob(jobId)).ThenReturn(mockJob);
			Org.Mockito.Mockito.When(mockContext.GetApplicationID()).ThenReturn(appId);
			Org.Mockito.Mockito.When(mockContext.IsLastAMRetry()).ThenReturn(isLastAMRetry);
			return mockContext;
		}

		private class TestParams
		{
			internal bool isLastAMRetry;

			internal string workDir = this._enclosing.SetupTestWorkDir();

			internal string dfsWorkDir = "/" + this.GetType().GetCanonicalName();

			internal ApplicationId appId = ApplicationId.NewInstance(200, 1);

			internal ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(this
				.appId, 1);

			internal ContainerId containerId = ContainerId.NewContainerId(this.appAttemptId, 
				1);

			internal TaskID taskID = TaskID.ForName("task_200707121733_0003_m_000005");

			internal TaskAttemptID taskAttemptID = new TaskAttemptID(this.taskID, 0);

			internal JobId jobId = MRBuilderUtils.NewJobId(this.appId, 1);

			internal AppContext mockAppContext;

			public TestParams(TestJobHistoryEventHandler _enclosing)
				: this(false)
			{
				this._enclosing = _enclosing;
			}

			public TestParams(TestJobHistoryEventHandler _enclosing, bool isLastAMRetry)
			{
				this._enclosing = _enclosing;
				this.isLastAMRetry = isLastAMRetry;
				this.mockAppContext = this._enclosing.MockAppContext(this.appId, this.isLastAMRetry
					);
			}

			private readonly TestJobHistoryEventHandler _enclosing;
		}

		private JobHistoryEvent GetEventToEnqueue(JobId jobId)
		{
			HistoryEvent toReturn = new JobStatusChangedEvent(new JobID(Sharpen.Extensions.ToString
				(jobId.GetId()), jobId.GetId()), "change status");
			return new JobHistoryEvent(jobId, toReturn);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSigTermedFunctionality()
		{
			AppContext mockedContext = Org.Mockito.Mockito.Mock<AppContext>();
			JHEventHandlerForSigtermTest jheh = new JHEventHandlerForSigtermTest(mockedContext
				, 0);
			JobId jobId = Org.Mockito.Mockito.Mock<JobId>();
			jheh.AddToFileMap(jobId);
			//Submit 4 events and check that they're handled in the absence of a signal
			int numEvents = 4;
			JobHistoryEvent[] events = new JobHistoryEvent[numEvents];
			for (int i = 0; i < numEvents; ++i)
			{
				events[i] = GetEventToEnqueue(jobId);
				jheh.Handle(events[i]);
			}
			jheh.Stop();
			//Make sure events were handled
			NUnit.Framework.Assert.IsTrue("handleEvent should've been called only 4 times but was "
				 + jheh.eventsHandled, jheh.eventsHandled == 4);
			//Create a new jheh because the last stop closed the eventWriter etc.
			jheh = new JHEventHandlerForSigtermTest(mockedContext, 0);
			// Make constructor of JobUnsuccessfulCompletionEvent pass
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockedContext.GetJob(jobId)).ThenReturn(job);
			// Make TypeConverter(JobID) pass
			ApplicationId mockAppId = Org.Mockito.Mockito.Mock<ApplicationId>();
			Org.Mockito.Mockito.When(mockAppId.GetClusterTimestamp()).ThenReturn(1000l);
			Org.Mockito.Mockito.When(jobId.GetAppId()).ThenReturn(mockAppId);
			jheh.AddToFileMap(jobId);
			jheh.SetForcejobCompletion(true);
			for (int i_1 = 0; i_1 < numEvents; ++i_1)
			{
				events[i_1] = GetEventToEnqueue(jobId);
				jheh.Handle(events[i_1]);
			}
			jheh.Stop();
			//Make sure events were handled, 4 + 1 finish event
			NUnit.Framework.Assert.IsTrue("handleEvent should've been called only 5 times but was "
				 + jheh.eventsHandled, jheh.eventsHandled == 5);
			NUnit.Framework.Assert.IsTrue("Last event handled wasn't JobUnsuccessfulCompletionEvent"
				, jheh.lastEventHandled.GetHistoryEvent() is JobUnsuccessfulCompletionEvent);
		}
	}

	internal class JHEvenHandlerForTest : JobHistoryEventHandler
	{
		private EventWriter eventWriter;

		private bool mockHistoryProcessing = true;

		public JHEvenHandlerForTest(AppContext context, int startCount)
			: base(context, startCount)
		{
			JobHistoryEventHandler.fileMap.Clear();
		}

		public JHEvenHandlerForTest(AppContext context, int startCount, bool mockHistoryProcessing
			)
			: base(context, startCount)
		{
			this.mockHistoryProcessing = mockHistoryProcessing;
			JobHistoryEventHandler.fileMap.Clear();
		}

		protected override void ServiceStart()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override EventWriter CreateEventWriter(Path historyFilePath)
		{
			if (mockHistoryProcessing)
			{
				this.eventWriter = Org.Mockito.Mockito.Mock<EventWriter>();
			}
			else
			{
				this.eventWriter = base.CreateEventWriter(historyFilePath);
			}
			return this.eventWriter;
		}

		protected internal override void CloseEventWriter(JobId jobId)
		{
		}

		public virtual EventWriter GetEventWriter()
		{
			return this.eventWriter;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessDoneFiles(JobId jobId)
		{
			if (!mockHistoryProcessing)
			{
				base.ProcessDoneFiles(jobId);
			}
		}
		// do nothing
	}

	/// <summary>Class to help with testSigTermedFunctionality</summary>
	internal class JHEventHandlerForSigtermTest : JobHistoryEventHandler
	{
		public JHEventHandlerForSigtermTest(AppContext context, int startCount)
			: base(context, startCount)
		{
		}

		public virtual void AddToFileMap(JobId jobId)
		{
			JobHistoryEventHandler.MetaInfo metaInfo = Org.Mockito.Mockito.Mock<JobHistoryEventHandler.MetaInfo
				>();
			Org.Mockito.Mockito.When(metaInfo.IsWriterActive()).ThenReturn(true);
			fileMap[jobId] = metaInfo;
		}

		internal JobHistoryEvent lastEventHandled;

		internal int eventsHandled = 0;

		public override void HandleEvent(JobHistoryEvent @event)
		{
			this.lastEventHandled = @event;
			this.eventsHandled++;
		}
	}
}
