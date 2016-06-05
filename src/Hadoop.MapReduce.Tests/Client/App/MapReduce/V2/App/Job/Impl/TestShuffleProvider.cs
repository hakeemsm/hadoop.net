using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class TestShuffleProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShuffleProviders()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestShuffleProvider.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(YarnConfiguration.NmAuxServices, TestShuffleProvider.TestShuffleHandler1
				.MapreduceTestShuffleServiceid + "," + TestShuffleProvider.TestShuffleHandler2.MapreduceTestShuffleServiceid
				);
			string serviceName = TestShuffleProvider.TestShuffleHandler1.MapreduceTestShuffleServiceid;
			string serviceStr = string.Format(YarnConfiguration.NmAuxServiceFmt, serviceName);
			jobConf.Set(serviceStr, typeof(TestShuffleProvider.TestShuffleHandler1).FullName);
			serviceName = TestShuffleProvider.TestShuffleHandler2.MapreduceTestShuffleServiceid;
			serviceStr = string.Format(YarnConfiguration.NmAuxServiceFmt, serviceName);
			jobConf.Set(serviceStr, typeof(TestShuffleProvider.TestShuffleHandler2).FullName);
			jobConf.Set(MRJobConfig.MapreduceJobShuffleProviderServices, TestShuffleProvider.TestShuffleHandler1
				.MapreduceTestShuffleServiceid + "," + TestShuffleProvider.TestShuffleHandler2.MapreduceTestShuffleServiceid
				);
			Credentials credentials = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken = new Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString(("tokenid")), Sharpen.Runtime.GetBytesForString
				(("tokenpw")), new Text("tokenkind"), new Text("tokenservice"));
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo>(), jobConf, taListener, 
				jobToken, credentials, new SystemClock(), null);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, taImpl.GetID().ToString());
			ContainerLaunchContext launchCtx = TaskAttemptImpl.CreateContainerLaunchContext(null
				, jobConf, jobToken, taImpl.CreateRemoteTask(), TypeConverter.FromYarn(jobId), Org.Mockito.Mockito.Mock
				<WrappedJvmID>(), taListener, credentials);
			IDictionary<string, ByteBuffer> serviceDataMap = launchCtx.GetServiceData();
			NUnit.Framework.Assert.IsNotNull("TestShuffleHandler1 is missing", serviceDataMap
				[TestShuffleProvider.TestShuffleHandler1.MapreduceTestShuffleServiceid]);
			NUnit.Framework.Assert.IsNotNull("TestShuffleHandler2 is missing", serviceDataMap
				[TestShuffleProvider.TestShuffleHandler2.MapreduceTestShuffleServiceid]);
			NUnit.Framework.Assert.IsTrue("mismatch number of services in map", serviceDataMap
				.Count == 3);
		}

		public class StubbedFS : RawLocalFileSystem
		{
			// 2 that we entered + 1 for the built-in shuffle-provider
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				return new FileStatus(1, false, 1, 1, 1, f);
			}
		}

		public class TestShuffleHandler1 : AuxiliaryService
		{
			public const string MapreduceTestShuffleServiceid = "test_shuffle1";

			public TestShuffleHandler1()
				: base("testshuffle1")
			{
			}

			public override void InitializeApplication(ApplicationInitializationContext context
				)
			{
			}

			public override void StopApplication(ApplicationTerminationContext context)
			{
			}

			public override ByteBuffer GetMetaData()
			{
				lock (this)
				{
					return ByteBuffer.Allocate(0);
				}
			}
			// Don't 'return null' because of YARN-1256
		}

		public class TestShuffleHandler2 : AuxiliaryService
		{
			public const string MapreduceTestShuffleServiceid = "test_shuffle2";

			public TestShuffleHandler2()
				: base("testshuffle2")
			{
			}

			public override void InitializeApplication(ApplicationInitializationContext context
				)
			{
			}

			public override void StopApplication(ApplicationTerminationContext context)
			{
			}

			public override ByteBuffer GetMetaData()
			{
				lock (this)
				{
					return ByteBuffer.Allocate(0);
				}
			}
			// Don't 'return null' because of YARN-1256
		}
	}
}
