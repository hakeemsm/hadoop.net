using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Class to test mapred task's
	/// - temp directory
	/// - child env
	/// </summary>
	public class TestMiniMRChildTask
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMiniMRChildTask).FullName
			);

		private const string OldConfigs = "test.old.configs";

		private const string TaskOptsVal = "-Xmx200m";

		private const string MapOptsVal = "-Xmx200m";

		private const string ReduceOptsVal = "-Xmx300m";

		private static MiniMRYarnCluster mr;

		private static MiniDFSCluster dfs;

		private static FileSystem fileSys;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		static TestMiniMRChildTask()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static Path TestRootDir = new Path("target", typeof(TestMiniMRChildTask).
			FullName + "-tmpDir").MakeQualified(localFs);

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		/// <summary>
		/// Map class which checks whether temp directory exists
		/// and check the value of java.io.tmpdir
		/// Creates a tempfile and checks whether that is created in
		/// temp directory specified.
		/// </summary>
		public class MapClass : MapReduceBase, Mapper<LongWritable, Text, Text, IntWritable
			>
		{
			internal Path tmpDir;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, IntWritable
				> output, Reporter reporter)
			{
				if (localFs.Exists(tmpDir))
				{
				}
				else
				{
					NUnit.Framework.Assert.Fail("Temp directory " + tmpDir + " doesnt exist.");
				}
				FilePath tmpFile = FilePath.CreateTempFile("test", ".tmp");
			}

			public override void Configure(JobConf job)
			{
				tmpDir = new Path(Runtime.GetProperty("java.io.tmpdir"));
				try
				{
					localFs = FileSystem.GetLocal(job);
				}
				catch (IOException ioe)
				{
					Sharpen.Runtime.PrintStackTrace(ioe);
					NUnit.Framework.Assert.Fail("IOException in getting localFS");
				}
			}
		}

		/// <summary>
		/// Map class which checks if hadoop lib location
		/// is in the execution path
		/// </summary>
		public class ExecutionEnvCheckMapClass : MapReduceBase, Mapper<LongWritable, Text
			, Text, IntWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, IntWritable
				> output, Reporter reporter)
			{
			}

			public override void Configure(JobConf job)
			{
				string executionEnvPathVariable = Runtime.Getenv(Shell.Windows ? "PATH" : "LD_LIBRARY_PATH"
					);
				string hadoopHome = Runtime.Getenv("HADOOP_COMMON_HOME");
				if (hadoopHome == null)
				{
					hadoopHome = string.Empty;
				}
				string hadoopLibLocation = hadoopHome + (Shell.Windows ? "\\bin" : "/lib/native");
				NUnit.Framework.Assert.IsTrue(executionEnvPathVariable.Contains(hadoopLibLocation
					));
			}
		}

		// configure a job
		/// <exception cref="System.IO.IOException"/>
		private void Configure(JobConf conf, Path inDir, Path outDir, string input, Type 
			map, Type reduce)
		{
			// set up the input file system and write input text.
			FileSystem inFs = inDir.GetFileSystem(conf);
			FileSystem outFs = outDir.GetFileSystem(conf);
			outFs.Delete(outDir, true);
			if (!inFs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				// write input into input file
				DataOutputStream file = inFs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			// configure the mapred Job which creates a tempfile in map.
			conf.SetJobName("testmap");
			conf.SetMapperClass(map);
			conf.SetReducerClass(reduce);
			conf.SetNumMapTasks(1);
			conf.SetNumReduceTasks(0);
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			string TestRootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).ToString
				().Replace(' ', '+');
			conf.Set("test.build.data", TestRootDir);
		}

		/// <summary>Launch tests</summary>
		/// <param name="conf">Configuration of the mapreduce job.</param>
		/// <param name="inDir">input path</param>
		/// <param name="outDir">output path</param>
		/// <param name="input">Input text</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void LaunchTest(JobConf conf, Path inDir, Path outDir, string input
			)
		{
			FileSystem outFs = outDir.GetFileSystem(conf);
			// Launch job with default option for temp dir. 
			// i.e. temp dir is ./tmp 
			Job job = Job.GetInstance(conf);
			job.AddFileToClassPath(AppJar);
			job.SetJarByClass(typeof(TestMiniMRChildTask));
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.WaitForCompletion(true);
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			outFs.Delete(outDir, true);
		}

		private static void CheckEnv(string envName, string expValue, string mode)
		{
			string envValue = Runtime.Getenv(envName).Trim();
			if ("append".Equals(mode))
			{
				if (envValue == null || !envValue.Contains(FilePath.pathSeparator))
				{
					throw new RuntimeException("Missing env variable");
				}
				else
				{
					string[] parts = envValue.Split(FilePath.pathSeparator);
					// check if the value is appended
					if (!parts[parts.Length - 1].Equals(expValue))
					{
						throw new RuntimeException("Wrong env variable in append mode");
					}
				}
			}
			else
			{
				if (envValue == null || !envValue.Equals(expValue))
				{
					throw new RuntimeException("Wrong env variable in noappend mode");
				}
			}
		}

		internal class EnvCheckMapper : MapReduceBase, Mapper<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			// Mappers that simply checks if the desired user env are present or not
			public override void Configure(JobConf job)
			{
				bool oldConfigs = job.GetBoolean(OldConfigs, false);
				if (oldConfigs)
				{
					string javaOpts = job.Get(JobConf.MapredTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredTaskJavaOpts + " is null!", javaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredTaskJavaOpts + " has value of: " + 
						javaOpts, javaOpts, TaskOptsVal);
				}
				else
				{
					string mapJavaOpts = job.Get(JobConf.MapredMapTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredMapTaskJavaOpts + " is null!", mapJavaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredMapTaskJavaOpts + " has value of: "
						 + mapJavaOpts, mapJavaOpts, MapOptsVal);
				}
				string path = job.Get("path");
				// check if the pwd is there in LD_LIBRARY_PATH
				string pwd = Runtime.Getenv("PWD");
				NUnit.Framework.Assert.IsTrue("LD doesnt contain pwd", Runtime.Getenv("LD_LIBRARY_PATH"
					).Contains(pwd));
				// check if X=$X:/abc works for LD_LIBRARY_PATH
				CheckEnv("LD_LIBRARY_PATH", "/tmp", "append");
				// check if X=y works for an already existing parameter
				CheckEnv("LANG", "en_us_8859_1", "noappend");
				// check if X=/tmp for a new env variable
				CheckEnv("MY_PATH", "/tmp", "noappend");
				// check if X=$X:/tmp works for a new env var and results into :/tmp
				CheckEnv("NEW_PATH", FilePath.pathSeparator + "/tmp", "noappend");
				// check if X=$(tt's X var):/tmp for an old env variable inherited from 
				// the tt
				if (Shell.Windows)
				{
					// On Windows, PATH is replaced one more time as part of default config
					// of "mapreduce.admin.user.env", i.e. on Windows,
					// "mapreduce.admin.user.env" is set to
					// "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin"
					string hadoopHome = Runtime.Getenv("HADOOP_COMMON_HOME");
					if (hadoopHome == null)
					{
						hadoopHome = string.Empty;
					}
					string hadoopLibLocation = hadoopHome + "\\bin";
					path += FilePath.pathSeparator + hadoopLibLocation;
					path += FilePath.pathSeparator + path;
				}
				CheckEnv("PATH", path + FilePath.pathSeparator + "/tmp", "noappend");
				string jobLocalDir = job.Get(MRJobConfig.JobLocalDir);
				NUnit.Framework.Assert.IsNotNull(MRJobConfig.JobLocalDir + " is null", jobLocalDir
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable value, OutputCollector<WritableComparable
				, Writable> @out, Reporter reporter)
			{
			}
		}

		internal class EnvCheckReducer : MapReduceBase, Reducer<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			public override void Configure(JobConf job)
			{
				bool oldConfigs = job.GetBoolean(OldConfigs, false);
				if (oldConfigs)
				{
					string javaOpts = job.Get(JobConf.MapredTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredTaskJavaOpts + " is null!", javaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredTaskJavaOpts + " has value of: " + 
						javaOpts, javaOpts, TaskOptsVal);
				}
				else
				{
					string reduceJavaOpts = job.Get(JobConf.MapredReduceTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredReduceTaskJavaOpts + " is null!", 
						reduceJavaOpts);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredReduceTaskJavaOpts + " has value of: "
						 + reduceJavaOpts, reduceJavaOpts, ReduceOptsVal);
				}
				string path = job.Get("path");
				// check if the pwd is there in LD_LIBRARY_PATH
				string pwd = Runtime.Getenv("PWD");
				NUnit.Framework.Assert.IsTrue("LD doesnt contain pwd", Runtime.Getenv("LD_LIBRARY_PATH"
					).Contains(pwd));
				// check if X=$X:/abc works for LD_LIBRARY_PATH
				CheckEnv("LD_LIBRARY_PATH", "/tmp", "append");
				// check if X=y works for an already existing parameter
				CheckEnv("LANG", "en_us_8859_1", "noappend");
				// check if X=/tmp for a new env variable
				CheckEnv("MY_PATH", "/tmp", "noappend");
				// check if X=$X:/tmp works for a new env var and results into :/tmp
				CheckEnv("NEW_PATH", FilePath.pathSeparator + "/tmp", "noappend");
				// check if X=$(tt's X var):/tmp for an old env variable inherited from 
				// the tt
				if (Shell.Windows)
				{
					// On Windows, PATH is replaced one more time as part of default config
					// of "mapreduce.admin.user.env", i.e. on Windows,
					// "mapreduce.admin.user.env"
					// is set to "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin"
					string hadoopHome = Runtime.Getenv("HADOOP_COMMON_HOME");
					if (hadoopHome == null)
					{
						hadoopHome = string.Empty;
					}
					string hadoopLibLocation = hadoopHome + "\\bin";
					path += FilePath.pathSeparator + hadoopLibLocation;
					path += FilePath.pathSeparator + path;
				}
				CheckEnv("PATH", path + FilePath.pathSeparator + "/tmp", "noappend");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(WritableComparable key, IEnumerator<Writable> values, 
				OutputCollector<WritableComparable, Writable> output, Reporter reporter)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			// create configuration, dfs, file system and mapred cluster 
			dfs = new MiniDFSCluster.Builder(conf).Build();
			fileSys = dfs.GetFileSystem();
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mr == null)
			{
				mr = new MiniMRYarnCluster(typeof(TestMiniMRChildTask).FullName);
				Configuration conf = new Configuration();
				mr.Init(conf);
				mr.Start();
			}
			// Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
			// workaround the absent public discache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), AppJar);
			localFs.SetPermission(AppJar, new FsPermission("700"));
		}

		[AfterClass]
		public static void TearDown()
		{
			// close file system and shut down dfs and mapred cluster
			try
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (dfs != null)
				{
					dfs.Shutdown();
				}
				if (mr != null)
				{
					mr.Stop();
					mr = null;
				}
			}
			catch (IOException ioe)
			{
				Log.Info("IO exception in closing file system)");
				Sharpen.Runtime.PrintStackTrace(ioe);
			}
		}

		/// <summary>To test OS dependent setting of default execution path for a MapRed task.
		/// 	</summary>
		/// <remarks>
		/// To test OS dependent setting of default execution path for a MapRed task.
		/// Mainly that we can use MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV to set -
		/// for WINDOWS: %HADOOP_COMMON_HOME%\bin is expected to be included in PATH - for
		/// Linux: $HADOOP_COMMON_HOME/lib/native is expected to be included in
		/// LD_LIBRARY_PATH
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestMapRedExecutionEnv()
		{
			// test if the env variable can be set
			try
			{
				// Application environment
				IDictionary<string, string> environment = new Dictionary<string, string>();
				string setupHadoopHomeCommand = Shell.Windows ? "HADOOP_COMMON_HOME=C:\\fake\\PATH\\to\\hadoop\\common\\home"
					 : "HADOOP_COMMON_HOME=/fake/path/to/hadoop/common/home";
				MRApps.SetEnvFromInputString(environment, setupHadoopHomeCommand, conf);
				// Add the env variables passed by the admin
				MRApps.SetEnvFromInputString(environment, conf.Get(MRJobConfig.MapredAdminUserEnv
					, MRJobConfig.DefaultMapredAdminUserEnv), conf);
				string executionPaths = environment[Shell.Windows ? "PATH" : "LD_LIBRARY_PATH"];
				string toFind = Shell.Windows ? "C:\\fake\\PATH\\to\\hadoop\\common\\home\\bin" : 
					"/fake/path/to/hadoop/common/home/lib/native";
				// Ensure execution PATH/LD_LIBRARY_PATH set up pointing to hadoop lib
				NUnit.Framework.Assert.IsTrue("execution path does not include the hadoop lib location "
					 + toFind, executionPaths.Contains(toFind));
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Exception in testing execution environment for MapReduce task"
					);
				TearDown();
			}
			// now launch a mapreduce job to ensure that the child 
			// also gets the configured setting for hadoop lib
			try
			{
				JobConf conf = new JobConf(mr.GetConfig());
				// initialize input, output directories
				Path inDir = new Path("input");
				Path outDir = new Path("output");
				string input = "The input";
				// set config to use the ExecutionEnvCheckMapClass map class
				Configure(conf, inDir, outDir, input, typeof(TestMiniMRChildTask.ExecutionEnvCheckMapClass
					), typeof(IdentityReducer));
				LaunchTest(conf, inDir, outDir, input);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Exception in testing propagation of env setting to child task"
					);
				TearDown();
			}
		}

		/// <summary>
		/// Test to test if the user set env variables reflect in the child
		/// processes.
		/// </summary>
		/// <remarks>
		/// Test to test if the user set env variables reflect in the child
		/// processes. Mainly
		/// - x=y (x can be a already existing env variable or a new variable)
		/// - x=$x:y (replace $x with the current value of x)
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestTaskEnv()
		{
			try
			{
				JobConf conf = new JobConf(mr.GetConfig());
				// initialize input, output directories
				Path inDir = new Path("testing/wc/input1");
				Path outDir = new Path("testing/wc/output1");
				FileSystem outFs = outDir.GetFileSystem(conf);
				RunTestTaskEnv(conf, inDir, outDir, false);
				outFs.Delete(outDir, true);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Exception in testing child env");
				TearDown();
			}
		}

		/// <summary>
		/// Test to test if the user set *old* env variables reflect in the child
		/// processes.
		/// </summary>
		/// <remarks>
		/// Test to test if the user set *old* env variables reflect in the child
		/// processes. Mainly
		/// - x=y (x can be a already existing env variable or a new variable)
		/// - x=$x:y (replace $x with the current value of x)
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestTaskOldEnv()
		{
			try
			{
				JobConf conf = new JobConf(mr.GetConfig());
				// initialize input, output directories
				Path inDir = new Path("testing/wc/input1");
				Path outDir = new Path("testing/wc/output1");
				FileSystem outFs = outDir.GetFileSystem(conf);
				RunTestTaskEnv(conf, inDir, outDir, true);
				outFs.Delete(outDir, true);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Exception in testing child env");
				TearDown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		internal virtual void RunTestTaskEnv(JobConf conf, Path inDir, Path outDir, bool 
			oldConfigs)
		{
			string input = "The input";
			Configure(conf, inDir, outDir, input, typeof(TestMiniMRChildTask.EnvCheckMapper), 
				typeof(TestMiniMRChildTask.EnvCheckReducer));
			// test 
			//  - new SET of new var (MY_PATH)
			//  - set of old var (LANG)
			//  - append to an old var from modified env (LD_LIBRARY_PATH)
			//  - append to an old var from tt's env (PATH)
			//  - append to a new var (NEW_PATH)
			string mapTaskEnvKey = JobConf.MapredMapTaskEnv;
			string reduceTaskEnvKey = JobConf.MapredMapTaskEnv;
			string mapTaskJavaOptsKey = JobConf.MapredMapTaskJavaOpts;
			string reduceTaskJavaOptsKey = JobConf.MapredReduceTaskJavaOpts;
			string mapTaskJavaOpts = MapOptsVal;
			string reduceTaskJavaOpts = ReduceOptsVal;
			conf.SetBoolean(OldConfigs, oldConfigs);
			if (oldConfigs)
			{
				mapTaskEnvKey = reduceTaskEnvKey = JobConf.MapredTaskEnv;
				mapTaskJavaOptsKey = reduceTaskJavaOptsKey = JobConf.MapredTaskJavaOpts;
				mapTaskJavaOpts = reduceTaskJavaOpts = TaskOptsVal;
			}
			conf.Set(mapTaskEnvKey, Shell.Windows ? "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=%LD_LIBRARY_PATH%;/tmp,"
				 + "PATH=%PATH%;/tmp,NEW_PATH=%NEW_PATH%;/tmp" : "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp,"
				 + "PATH=$PATH:/tmp,NEW_PATH=$NEW_PATH:/tmp");
			conf.Set(reduceTaskEnvKey, Shell.Windows ? "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=%LD_LIBRARY_PATH%;/tmp,"
				 + "PATH=%PATH%;/tmp,NEW_PATH=%NEW_PATH%;/tmp" : "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp,"
				 + "PATH=$PATH:/tmp,NEW_PATH=$NEW_PATH:/tmp");
			conf.Set("path", Runtime.Getenv("PATH"));
			conf.Set(mapTaskJavaOptsKey, mapTaskJavaOpts);
			conf.Set(reduceTaskJavaOptsKey, reduceTaskJavaOpts);
			Job job = Job.GetInstance(conf);
			job.AddFileToClassPath(AppJar);
			job.SetJarByClass(typeof(TestMiniMRChildTask));
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.WaitForCompletion(true);
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("The environment checker job failed.", succeeded);
		}
	}
}
