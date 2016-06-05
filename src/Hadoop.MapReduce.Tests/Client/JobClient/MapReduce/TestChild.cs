using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestChild : HadoopTestCase
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+');

		private readonly Path inDir = new Path(TestRootDir, "./wc/input");

		private readonly Path outDir = new Path(TestRootDir, "./wc/output");

		private const string OldConfigs = "test.old.configs";

		private const string TaskOptsVal = "-Xmx200m";

		private const string MapOptsVal = "-Xmx200m";

		private const string ReduceOptsVal = "-Xmx300m";

		/// <exception cref="System.IO.IOException"/>
		public TestChild()
			: base(HadoopTestCase.ClusterMr, HadoopTestCase.LocalFs, 2, 2)
		{
		}

		internal class MyMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				bool oldConfigs = conf.GetBoolean(OldConfigs, false);
				if (oldConfigs)
				{
					string javaOpts = conf.Get(JobConf.MapredTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredTaskJavaOpts + " is null!", javaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredTaskJavaOpts + " has value of: " + 
						javaOpts, javaOpts, TaskOptsVal);
				}
				else
				{
					string mapJavaOpts = conf.Get(JobConf.MapredMapTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredMapTaskJavaOpts + " is null!", mapJavaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredMapTaskJavaOpts + " has value of: "
						 + mapJavaOpts, mapJavaOpts, MapOptsVal);
				}
				Level logLevel = Level.ToLevel(conf.Get(JobConf.MapredMapTaskLogLevel, Level.Info
					.ToString()));
				NUnit.Framework.Assert.AreEqual(JobConf.MapredMapTaskLogLevel + "has value of " +
					 logLevel, logLevel, Level.Off);
			}
		}

		internal class MyReducer : Reducer<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Reducer.Context context)
			{
				Configuration conf = context.GetConfiguration();
				bool oldConfigs = conf.GetBoolean(OldConfigs, false);
				if (oldConfigs)
				{
					string javaOpts = conf.Get(JobConf.MapredTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredTaskJavaOpts + " is null!", javaOpts
						);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredTaskJavaOpts + " has value of: " + 
						javaOpts, javaOpts, TaskOptsVal);
				}
				else
				{
					string reduceJavaOpts = conf.Get(JobConf.MapredReduceTaskJavaOpts);
					NUnit.Framework.Assert.IsNotNull(JobConf.MapredReduceTaskJavaOpts + " is null!", 
						reduceJavaOpts);
					NUnit.Framework.Assert.AreEqual(JobConf.MapredReduceTaskJavaOpts + " has value of: "
						 + reduceJavaOpts, reduceJavaOpts, ReduceOptsVal);
				}
				Level logLevel = Level.ToLevel(conf.Get(JobConf.MapredReduceTaskLogLevel, Level.Info
					.ToString()));
				NUnit.Framework.Assert.AreEqual(JobConf.MapredReduceTaskLogLevel + "has value of "
					 + logLevel, logLevel, Level.Off);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private Job SubmitAndValidateJob(JobConf conf, int numMaps, int numReds, bool oldConfigs
			)
		{
			conf.SetBoolean(OldConfigs, oldConfigs);
			if (oldConfigs)
			{
				conf.Set(JobConf.MapredTaskJavaOpts, TaskOptsVal);
			}
			else
			{
				conf.Set(JobConf.MapredMapTaskJavaOpts, MapOptsVal);
				conf.Set(JobConf.MapredReduceTaskJavaOpts, ReduceOptsVal);
			}
			conf.Set(JobConf.MapredMapTaskLogLevel, Level.Off.ToString());
			conf.Set(JobConf.MapredReduceTaskLogLevel, Level.Off.ToString());
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, numMaps, numReds);
			job.SetMapperClass(typeof(TestChild.MyMapper));
			job.SetReducerClass(typeof(TestChild.MyReducer));
			NUnit.Framework.Assert.IsFalse("Job already has a job tracker connection, before it's submitted"
				, job.IsConnected());
			job.Submit();
			NUnit.Framework.Assert.IsTrue("Job doesn't have a job tracker connection, even though it's been submitted"
				, job.IsConnected());
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			// Check output directory
			FileSystem fs = FileSystem.Get(conf);
			NUnit.Framework.Assert.IsTrue("Job output directory doesn't exit!", fs.Exists(outDir
				));
			FileStatus[] list = fs.ListStatus(outDir, new TestChild.OutputFilter());
			int numPartFiles = numReds == 0 ? numMaps : numReds;
			NUnit.Framework.Assert.IsTrue("Number of part-files is " + list.Length + " and not "
				 + numPartFiles, list.Length == numPartFiles);
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestChild()
		{
			try
			{
				SubmitAndValidateJob(CreateJobConf(), 1, 1, true);
				SubmitAndValidateJob(CreateJobConf(), 1, 1, false);
			}
			finally
			{
				TearDown();
			}
		}

		private class OutputFilter : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				return !(path.GetName().StartsWith("_"));
			}
		}
	}
}
