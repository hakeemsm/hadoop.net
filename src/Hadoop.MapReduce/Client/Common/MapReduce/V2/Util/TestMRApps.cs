using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Util
{
	public class TestMRApps
	{
		private static FilePath testWorkDir = null;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRApps));

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupTestDirs()
		{
			testWorkDir = new FilePath("target", typeof(TestMRApps).GetCanonicalName());
			Delete(testWorkDir);
			testWorkDir.Mkdirs();
			testWorkDir = testWorkDir.GetAbsoluteFile();
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

		/// <exception cref="System.IO.IOException"/>
		private static void Delete(FilePath dir)
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path p = fs.MakeQualified(new Path(dir.GetAbsolutePath()));
			fs.Delete(p, true);
		}

		public virtual void TestJobIDtoString()
		{
			JobId jid = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<JobId>
				();
			jid.SetAppId(ApplicationId.NewInstance(0, 0));
			NUnit.Framework.Assert.AreEqual("job_0_0000", MRApps.ToString(jid));
		}

		public virtual void TestToJobID()
		{
			JobId jid = MRApps.ToJobID("job_1_1");
			NUnit.Framework.Assert.AreEqual(1, jid.GetAppId().GetClusterTimestamp());
			NUnit.Framework.Assert.AreEqual(1, jid.GetAppId().GetId());
			NUnit.Framework.Assert.AreEqual(1, jid.GetId());
		}

		// tests against some proto.id and not a job.id field
		public virtual void TestJobIDShort()
		{
			MRApps.ToJobID("job_0_0_0");
		}

		//TODO_get.set
		public virtual void TestTaskIDtoString()
		{
			TaskId tid = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<TaskId
				>();
			tid.SetJobId(RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<JobId
				>());
			tid.GetJobId().SetAppId(ApplicationId.NewInstance(0, 0));
			tid.SetTaskType(TaskType.Map);
			TaskType type = tid.GetTaskType();
			System.Console.Error.WriteLine(type);
			type = TaskType.Reduce;
			System.Console.Error.WriteLine(type);
			System.Console.Error.WriteLine(tid.GetTaskType());
			NUnit.Framework.Assert.AreEqual("task_0_0000_m_000000", MRApps.ToString(tid));
			tid.SetTaskType(TaskType.Reduce);
			NUnit.Framework.Assert.AreEqual("task_0_0000_r_000000", MRApps.ToString(tid));
		}

		public virtual void TestToTaskID()
		{
			TaskId tid = MRApps.ToTaskID("task_1_2_r_3");
			NUnit.Framework.Assert.AreEqual(1, tid.GetJobId().GetAppId().GetClusterTimestamp(
				));
			NUnit.Framework.Assert.AreEqual(2, tid.GetJobId().GetAppId().GetId());
			NUnit.Framework.Assert.AreEqual(2, tid.GetJobId().GetId());
			NUnit.Framework.Assert.AreEqual(TaskType.Reduce, tid.GetTaskType());
			NUnit.Framework.Assert.AreEqual(3, tid.GetId());
			tid = MRApps.ToTaskID("task_1_2_m_3");
			NUnit.Framework.Assert.AreEqual(TaskType.Map, tid.GetTaskType());
		}

		public virtual void TestTaskIDShort()
		{
			MRApps.ToTaskID("task_0_0000_m");
		}

		public virtual void TestTaskIDBadType()
		{
			MRApps.ToTaskID("task_0_0000_x_000000");
		}

		//TODO_get.set
		public virtual void TestTaskAttemptIDtoString()
		{
			TaskAttemptId taid = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance
				<TaskAttemptId>();
			taid.SetTaskId(RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<TaskId
				>());
			taid.GetTaskId().SetTaskType(TaskType.Map);
			taid.GetTaskId().SetJobId(RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance
				<JobId>());
			taid.GetTaskId().GetJobId().SetAppId(ApplicationId.NewInstance(0, 0));
			NUnit.Framework.Assert.AreEqual("attempt_0_0000_m_000000_0", MRApps.ToString(taid
				));
		}

		public virtual void TestToTaskAttemptID()
		{
			TaskAttemptId taid = MRApps.ToTaskAttemptID("attempt_0_1_m_2_3");
			NUnit.Framework.Assert.AreEqual(0, taid.GetTaskId().GetJobId().GetAppId().GetClusterTimestamp
				());
			NUnit.Framework.Assert.AreEqual(1, taid.GetTaskId().GetJobId().GetAppId().GetId()
				);
			NUnit.Framework.Assert.AreEqual(1, taid.GetTaskId().GetJobId().GetId());
			NUnit.Framework.Assert.AreEqual(2, taid.GetTaskId().GetId());
			NUnit.Framework.Assert.AreEqual(3, taid.GetId());
		}

		public virtual void TestTaskAttemptIDShort()
		{
			MRApps.ToTaskAttemptID("attempt_0_0_0_m_0");
		}

		public virtual void TestGetJobFileWithUser()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, "/my/path/to/staging");
			string jobFile = MRApps.GetJobFile(conf, "dummy-user", new JobID("dummy-job", 12345
				));
			NUnit.Framework.Assert.IsNotNull("getJobFile results in null.", jobFile);
			NUnit.Framework.Assert.AreEqual("jobFile with specified user is not as expected."
				, "/my/path/to/staging/dummy-user/.staging/job_dummy-job_12345/job.xml", jobFile
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetClasspath()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			Job job = Job.GetInstance(conf);
			IDictionary<string, string> environment = new Dictionary<string, string>();
			MRApps.SetClasspath(environment, job.GetConfiguration());
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].StartsWith(ApplicationConstants.Environment.Pwd.$$() + ApplicationConstants
				.ClassPathSeparator));
			string yarnAppClasspath = job.GetConfiguration().Get(YarnConfiguration.YarnApplicationClasspath
				, StringUtils.Join(",", YarnConfiguration.DefaultYarnCrossPlatformApplicationClasspath
				));
			if (yarnAppClasspath != null)
			{
				yarnAppClasspath = yarnAppClasspath.ReplaceAll(",\\s*", ApplicationConstants.ClassPathSeparator
					).Trim();
			}
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].Contains(yarnAppClasspath));
			string mrAppClasspath = job.GetConfiguration().Get(MRJobConfig.MapreduceApplicationClasspath
				, MRJobConfig.DefaultMapreduceCrossPlatformApplicationClasspath);
			if (mrAppClasspath != null)
			{
				mrAppClasspath = mrAppClasspath.ReplaceAll(",\\s*", ApplicationConstants.ClassPathSeparator
					).Trim();
			}
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].Contains(mrAppClasspath));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetClasspathWithArchives()
		{
			FilePath testTGZ = new FilePath(testWorkDir, "test.tgz");
			FileOutputStream @out = new FileOutputStream(testTGZ);
			@out.Write(0);
			@out.Close();
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			string testTGZQualifiedPath = FileSystem.GetLocal(conf).MakeQualified(new Path(testTGZ
				.GetAbsolutePath())).ToString();
			conf.Set(MRJobConfig.ClasspathArchives, testTGZQualifiedPath);
			conf.Set(MRJobConfig.CacheArchives, testTGZQualifiedPath + "#testTGZ");
			IDictionary<string, string> environment = new Dictionary<string, string>();
			MRApps.SetClasspath(environment, conf);
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].StartsWith(ApplicationConstants.Environment.Pwd.$$() + ApplicationConstants
				.ClassPathSeparator));
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.HadoopClasspath
				.ToString()].StartsWith(ApplicationConstants.Environment.Pwd.$$() + ApplicationConstants
				.ClassPathSeparator));
			string confClasspath = job.GetConfiguration().Get(YarnConfiguration.YarnApplicationClasspath
				, StringUtils.Join(",", YarnConfiguration.DefaultYarnCrossPlatformApplicationClasspath
				));
			if (confClasspath != null)
			{
				confClasspath = confClasspath.ReplaceAll(",\\s*", ApplicationConstants.ClassPathSeparator
					).Trim();
			}
			Log.Info("CLASSPATH: " + environment[ApplicationConstants.Environment.Classpath.ToString
				()]);
			Log.Info("confClasspath: " + confClasspath);
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].Contains(confClasspath));
			Log.Info("HADOOP_CLASSPATH: " + environment[ApplicationConstants.Environment.HadoopClasspath
				.ToString()]);
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.Classpath
				.ToString()].Contains("testTGZ"));
			NUnit.Framework.Assert.IsTrue(environment[ApplicationConstants.Environment.HadoopClasspath
				.ToString()].Contains("testTGZ"));
		}

		public virtual void TestSetClasspathWithUserPrecendence()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobUserClasspathFirst, true);
			IDictionary<string, string> env = new Dictionary<string, string>();
			try
			{
				MRApps.SetClasspath(env, conf);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Got exception while setting classpath");
			}
			string env_str = env[ApplicationConstants.Environment.Classpath.ToString()];
			string expectedClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator
				, Arrays.AsList(ApplicationConstants.Environment.Pwd.$$(), "job.jar/job.jar", "job.jar/classes/"
				, "job.jar/lib/*", ApplicationConstants.Environment.Pwd.$$() + "/*"));
			NUnit.Framework.Assert.IsTrue("MAPREDUCE_JOB_USER_CLASSPATH_FIRST set, but not taking effect!"
				, env_str.StartsWith(expectedClasspath));
		}

		public virtual void TestSetClasspathWithNoUserPrecendence()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobUserClasspathFirst, false);
			IDictionary<string, string> env = new Dictionary<string, string>();
			try
			{
				MRApps.SetClasspath(env, conf);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Got exception while setting classpath");
			}
			string env_str = env[ApplicationConstants.Environment.Classpath.ToString()];
			string expectedClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator
				, Arrays.AsList("job.jar/job.jar", "job.jar/classes/", "job.jar/lib/*", ApplicationConstants.Environment
				.Pwd.$$() + "/*"));
			NUnit.Framework.Assert.IsTrue("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, and job.jar is not in"
				 + " the classpath!", env_str.Contains(expectedClasspath));
			NUnit.Framework.Assert.IsFalse("MAPREDUCE_JOB_USER_CLASSPATH_FIRST false, but taking effect!"
				, env_str.StartsWith(expectedClasspath));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetClasspathWithJobClassloader()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			conf.SetBoolean(MRJobConfig.MapreduceJobClassloader, true);
			IDictionary<string, string> env = new Dictionary<string, string>();
			MRApps.SetClasspath(env, conf);
			string cp = env[ApplicationConstants.Environment.Classpath.ToString()];
			string appCp = env[ApplicationConstants.Environment.AppClasspath.ToString()];
			NUnit.Framework.Assert.IsFalse("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is in the"
				 + " classpath!", cp.Contains("jar" + ApplicationConstants.ClassPathSeparator + 
				"job"));
			NUnit.Framework.Assert.IsFalse("MAPREDUCE_JOB_CLASSLOADER true, but PWD is in the classpath!"
				, cp.Contains("PWD"));
			string expectedAppClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator
				, Arrays.AsList(ApplicationConstants.Environment.Pwd.$$(), "job.jar/job.jar", "job.jar/classes/"
				, "job.jar/lib/*", ApplicationConstants.Environment.Pwd.$$() + "/*"));
			NUnit.Framework.Assert.AreEqual("MAPREDUCE_JOB_CLASSLOADER true, but job.jar is not in the app"
				 + " classpath!", expectedAppClasspath, appCp);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetClasspathWithFramework()
		{
			string FrameworkName = "some-framework-name";
			string FrameworkPath = "some-framework-path#" + FrameworkName;
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			conf.Set(MRJobConfig.MapreduceApplicationFrameworkPath, FrameworkPath);
			IDictionary<string, string> env = new Dictionary<string, string>();
			try
			{
				MRApps.SetClasspath(env, conf);
				NUnit.Framework.Assert.Fail("Failed to catch framework path set without classpath change"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected IllegalArgumentException", e.Message.Contains
					("Could not locate MapReduce framework name '" + FrameworkName + "'"));
			}
			env.Clear();
			string FrameworkClasspath = FrameworkName + "/*.jar";
			conf.Set(MRJobConfig.MapreduceApplicationClasspath, FrameworkClasspath);
			MRApps.SetClasspath(env, conf);
			string stdClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator, Arrays
				.AsList("job.jar/job.jar", "job.jar/classes/", "job.jar/lib/*", ApplicationConstants.Environment
				.Pwd.$$() + "/*"));
			string expectedClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator
				, Arrays.AsList(ApplicationConstants.Environment.Pwd.$$(), FrameworkClasspath, stdClasspath
				));
			NUnit.Framework.Assert.AreEqual("Incorrect classpath with framework and no user precedence"
				, expectedClasspath, env[ApplicationConstants.Environment.Classpath.ToString()]);
			env.Clear();
			conf.SetBoolean(MRJobConfig.MapreduceJobUserClasspathFirst, true);
			MRApps.SetClasspath(env, conf);
			expectedClasspath = StringUtils.Join(ApplicationConstants.ClassPathSeparator, Arrays
				.AsList(ApplicationConstants.Environment.Pwd.$$(), stdClasspath, FrameworkClasspath
				));
			NUnit.Framework.Assert.AreEqual("Incorrect classpath with framework and user precedence"
				, expectedClasspath, env[ApplicationConstants.Environment.Classpath.ToString()]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetupDistributedCacheEmpty()
		{
			Configuration conf = new Configuration();
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			MRApps.SetupDistributedCache(conf, localResources);
			NUnit.Framework.Assert.IsTrue("Empty Config did not produce an empty list of resources"
				, localResources.IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetupDistributedCacheConflicts()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestMRApps.MockFileSystem), typeof(FileSystem
				));
			URI mockUri = URI.Create("mockfs://mock/");
			FileSystem mockFs = ((FilterFileSystem)FileSystem.Get(mockUri, conf)).GetRawFileSystem
				();
			URI archive = new URI("mockfs://mock/tmp/something.zip#something");
			Path archivePath = new Path(archive);
			URI file = new URI("mockfs://mock/tmp/something.txt#something");
			Path filePath = new Path(file);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(archivePath)).ThenReturn(archivePath);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(filePath)).ThenReturn(filePath);
			DistributedCache.AddCacheArchive(archive, conf);
			conf.Set(MRJobConfig.CacheArchivesTimestamps, "10");
			conf.Set(MRJobConfig.CacheArchivesSizes, "10");
			conf.Set(MRJobConfig.CacheArchivesVisibilities, "true");
			DistributedCache.AddCacheFile(file, conf);
			conf.Set(MRJobConfig.CacheFileTimestamps, "11");
			conf.Set(MRJobConfig.CacheFilesSizes, "11");
			conf.Set(MRJobConfig.CacheFileVisibilities, "true");
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			MRApps.SetupDistributedCache(conf, localResources);
			NUnit.Framework.Assert.AreEqual(1, localResources.Count);
			LocalResource lr = localResources["something"];
			//Archive wins
			NUnit.Framework.Assert.IsNotNull(lr);
			NUnit.Framework.Assert.AreEqual(10l, lr.GetSize());
			NUnit.Framework.Assert.AreEqual(10l, lr.GetTimestamp());
			NUnit.Framework.Assert.AreEqual(LocalResourceType.Archive, lr.GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetupDistributedCacheConflictsFiles()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestMRApps.MockFileSystem), typeof(FileSystem
				));
			URI mockUri = URI.Create("mockfs://mock/");
			FileSystem mockFs = ((FilterFileSystem)FileSystem.Get(mockUri, conf)).GetRawFileSystem
				();
			URI file = new URI("mockfs://mock/tmp/something.zip#something");
			Path filePath = new Path(file);
			URI file2 = new URI("mockfs://mock/tmp/something.txt#something");
			Path file2Path = new Path(file2);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(filePath)).ThenReturn(filePath);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(file2Path)).ThenReturn(file2Path);
			DistributedCache.AddCacheFile(file, conf);
			DistributedCache.AddCacheFile(file2, conf);
			conf.Set(MRJobConfig.CacheFileTimestamps, "10,11");
			conf.Set(MRJobConfig.CacheFilesSizes, "10,11");
			conf.Set(MRJobConfig.CacheFileVisibilities, "true,true");
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			MRApps.SetupDistributedCache(conf, localResources);
			NUnit.Framework.Assert.AreEqual(1, localResources.Count);
			LocalResource lr = localResources["something"];
			//First one wins
			NUnit.Framework.Assert.IsNotNull(lr);
			NUnit.Framework.Assert.AreEqual(10l, lr.GetSize());
			NUnit.Framework.Assert.AreEqual(10l, lr.GetTimestamp());
			NUnit.Framework.Assert.AreEqual(LocalResourceType.File, lr.GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetupDistributedCache()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestMRApps.MockFileSystem), typeof(FileSystem
				));
			URI mockUri = URI.Create("mockfs://mock/");
			FileSystem mockFs = ((FilterFileSystem)FileSystem.Get(mockUri, conf)).GetRawFileSystem
				();
			URI archive = new URI("mockfs://mock/tmp/something.zip");
			Path archivePath = new Path(archive);
			URI file = new URI("mockfs://mock/tmp/something.txt#something");
			Path filePath = new Path(file);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(archivePath)).ThenReturn(archivePath);
			Org.Mockito.Mockito.When(mockFs.ResolvePath(filePath)).ThenReturn(filePath);
			DistributedCache.AddCacheArchive(archive, conf);
			conf.Set(MRJobConfig.CacheArchivesTimestamps, "10");
			conf.Set(MRJobConfig.CacheArchivesSizes, "10");
			conf.Set(MRJobConfig.CacheArchivesVisibilities, "true");
			DistributedCache.AddCacheFile(file, conf);
			conf.Set(MRJobConfig.CacheFileTimestamps, "11");
			conf.Set(MRJobConfig.CacheFilesSizes, "11");
			conf.Set(MRJobConfig.CacheFileVisibilities, "true");
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			MRApps.SetupDistributedCache(conf, localResources);
			NUnit.Framework.Assert.AreEqual(2, localResources.Count);
			LocalResource lr = localResources["something.zip"];
			NUnit.Framework.Assert.IsNotNull(lr);
			NUnit.Framework.Assert.AreEqual(10l, lr.GetSize());
			NUnit.Framework.Assert.AreEqual(10l, lr.GetTimestamp());
			NUnit.Framework.Assert.AreEqual(LocalResourceType.Archive, lr.GetType());
			lr = localResources["something"];
			NUnit.Framework.Assert.IsNotNull(lr);
			NUnit.Framework.Assert.AreEqual(11l, lr.GetSize());
			NUnit.Framework.Assert.AreEqual(11l, lr.GetTimestamp());
			NUnit.Framework.Assert.AreEqual(LocalResourceType.File, lr.GetType());
		}

		internal class MockFileSystem : FilterFileSystem
		{
			internal MockFileSystem()
				: base(Org.Mockito.Mockito.Mock<FileSystem>())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskStateUI()
		{
			NUnit.Framework.Assert.IsTrue(MRApps.TaskStateUI.Pending.CorrespondsTo(TaskState.
				Scheduled));
			NUnit.Framework.Assert.IsTrue(MRApps.TaskStateUI.Completed.CorrespondsTo(TaskState
				.Succeeded));
			NUnit.Framework.Assert.IsTrue(MRApps.TaskStateUI.Completed.CorrespondsTo(TaskState
				.Failed));
			NUnit.Framework.Assert.IsTrue(MRApps.TaskStateUI.Completed.CorrespondsTo(TaskState
				.Killed));
			NUnit.Framework.Assert.IsTrue(MRApps.TaskStateUI.Running.CorrespondsTo(TaskState.
				Running));
		}

		private static readonly string[] SysClasses = new string[] { "/java/fake/Klass", 
			"/javax/management/fake/Klass", "/org/apache/commons/logging/fake/Klass", "/org/apache/log4j/fake/Klass"
			, "/org/apache/hadoop/fake/Klass" };

		private static readonly string[] DefaultXmls = new string[] { "core-default.xml", 
			"mapred-default.xml", "hdfs-default.xml", "yarn-default.xml" };

		[NUnit.Framework.Test]
		public virtual void TestSystemClasses()
		{
			IList<string> systemClasses = Arrays.AsList(StringUtils.GetTrimmedStrings(ApplicationClassLoader
				.SystemClassesDefault));
			foreach (string defaultXml in DefaultXmls)
			{
				NUnit.Framework.Assert.IsTrue(defaultXml + " must be system resource", ApplicationClassLoader
					.IsSystemClass(defaultXml, systemClasses));
			}
			foreach (string klass in SysClasses)
			{
				NUnit.Framework.Assert.IsTrue(klass + " must be system class", ApplicationClassLoader
					.IsSystemClass(klass, systemClasses));
			}
			NUnit.Framework.Assert.IsFalse("/fake/Klass must not be a system class", ApplicationClassLoader
				.IsSystemClass("/fake/Klass", systemClasses));
		}
	}
}
