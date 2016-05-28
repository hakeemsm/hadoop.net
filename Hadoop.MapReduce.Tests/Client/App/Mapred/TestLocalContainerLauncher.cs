using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestLocalContainerLauncher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLocalContainerLauncher
			));

		private static FilePath testWorkDir;

		private static readonly string[] localDirs = new string[2];

		/// <exception cref="System.IO.IOException"/>
		private static void Delete(FilePath dir)
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path p = fs.MakeQualified(new Path(dir.GetAbsolutePath()));
			fs.Delete(p, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupTestDirs()
		{
			testWorkDir = new FilePath("target", typeof(TestLocalContainerLauncher).GetCanonicalName
				());
			testWorkDir.Delete();
			testWorkDir.Mkdirs();
			testWorkDir = testWorkDir.GetAbsoluteFile();
			for (int i = 0; i < localDirs.Length; i++)
			{
				FilePath dir = new FilePath(testWorkDir, "local-" + i);
				dir.Mkdirs();
				localDirs[i] = dir.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void CleanupTestDirs()
		{
			if (testWorkDir != null)
			{
				Delete(testWorkDir);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKillJob()
		{
			JobConf conf = new JobConf();
			AppContext context = Org.Mockito.Mockito.Mock<AppContext>();
			// a simple event handler solely to detect the container cleaned event
			CountDownLatch isDone = new CountDownLatch(1);
			EventHandler handler = new _EventHandler_106(isDone);
			Org.Mockito.Mockito.When(context.GetEventHandler()).ThenReturn(handler);
			// create and start the launcher
			LocalContainerLauncher launcher = new LocalContainerLauncher(context, Org.Mockito.Mockito.Mock
				<TaskUmbilicalProtocol>());
			launcher.Init(conf);
			launcher.Start();
			// create mocked job, task, and task attempt
			// a single-mapper job
			JobId jobId = MRBuilderUtils.NewJobId(Runtime.CurrentTimeMillis(), 1, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId taId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(job.GetTotalMaps()).ThenReturn(1);
			Org.Mockito.Mockito.When(job.GetTotalReduces()).ThenReturn(0);
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs = new Dictionary
				<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
			jobs[jobId] = job;
			// app context returns the one and only job
			Org.Mockito.Mockito.When(context.GetAllJobs()).ThenReturn(jobs);
			Task ytask = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(ytask.GetType()).ThenReturn(TaskType.Map);
			Org.Mockito.Mockito.When(job.GetTask(taskId)).ThenReturn(ytask);
			// create a sleeping mapper that runs beyond the test timeout
			MapTask mapTask = Org.Mockito.Mockito.Mock<MapTask>();
			Org.Mockito.Mockito.When(mapTask.IsMapOrReduce()).ThenReturn(true);
			Org.Mockito.Mockito.When(mapTask.IsMapTask()).ThenReturn(true);
			TaskAttemptID taskID = TypeConverter.FromYarn(taId);
			Org.Mockito.Mockito.When(mapTask.GetTaskID()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mapTask.GetJobID()).ThenReturn(((JobID)taskID.GetJobID()
				));
			Org.Mockito.Mockito.DoAnswer(new _Answer_152()).When(mapTask).Run(Matchers.IsA<JobConf
				>(), Matchers.IsA<TaskUmbilicalProtocol>());
			// sleep for a long time
			// pump in a task attempt launch event
			ContainerLauncherEvent launchEvent = new ContainerRemoteLaunchEvent(taId, null, CreateMockContainer
				(), mapTask);
			launcher.Handle(launchEvent);
			Sharpen.Thread.Sleep(200);
			// now pump in a container clean-up event
			ContainerLauncherEvent cleanupEvent = new ContainerLauncherEvent(taId, null, null
				, null, ContainerLauncher.EventType.ContainerRemoteCleanup);
			launcher.Handle(cleanupEvent);
			// wait for the event to fire: this should be received promptly
			isDone.Await();
			launcher.Close();
		}

		private sealed class _EventHandler_106 : EventHandler
		{
			public _EventHandler_106(CountDownLatch isDone)
			{
				this.isDone = isDone;
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				TestLocalContainerLauncher.Log.Info("handling event " + @event.GetType() + " with type "
					 + @event.GetType());
				if (@event is TaskAttemptEvent)
				{
					if (@event.GetType() == TaskAttemptEventType.TaContainerCleaned)
					{
						isDone.CountDown();
					}
				}
			}

			private readonly CountDownLatch isDone;
		}

		private sealed class _Answer_152 : Answer<Void>
		{
			public _Answer_152()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				TestLocalContainerLauncher.Log.Info("sleeping for 5 minutes...");
				Sharpen.Thread.Sleep(5 * 60 * 1000);
				return null;
			}
		}

		private static Container CreateMockContainer()
		{
			Container container = Org.Mockito.Mockito.Mock<Container>();
			NodeId nodeId = NodeId.NewInstance("foo.bar.org", 1234);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nodeId);
			return container;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameMapOutputForReduce()
		{
			JobConf conf = new JobConf();
			MROutputFiles mrOutputFiles = new MROutputFiles();
			mrOutputFiles.SetConf(conf);
			// make sure both dirs are distinct
			//
			conf.Set(MRConfig.LocalDir, localDirs[0].ToString());
			Path mapOut = mrOutputFiles.GetOutputFileForWrite(1);
			conf.Set(MRConfig.LocalDir, localDirs[1].ToString());
			Path mapOutIdx = mrOutputFiles.GetOutputIndexFileForWrite(1);
			Assert.AssertNotEquals("Paths must be different!", mapOut.GetParent(), mapOutIdx.
				GetParent());
			// make both dirs part of LOCAL_DIR
			conf.SetStrings(MRConfig.LocalDir, localDirs);
			FileContext lfc = FileContext.GetLocalFSFileContext(conf);
			lfc.Create(mapOut, EnumSet.Of(CreateFlag.Create)).Close();
			lfc.Create(mapOutIdx, EnumSet.Of(CreateFlag.Create)).Close();
			JobId jobId = MRBuilderUtils.NewJobId(12345L, 1, 2);
			TaskId tid = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			TaskAttemptId taid = MRBuilderUtils.NewTaskAttemptId(tid, 0);
			LocalContainerLauncher.RenameMapOutputForReduce(conf, taid, mrOutputFiles);
		}
	}
}
