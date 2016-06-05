using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRCJCJobClient : TestMRJobClient
	{
		/// <exception cref="System.Exception"/>
		private string RunJob()
		{
			OutputStream os = GetFileSystem().Create(new Path(GetInputDir(), "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("hello1\n");
			wr.Write("hello2\n");
			wr.Write("hello3\n");
			wr.Close();
			JobConf conf = CreateJobConf();
			conf.SetJobName("mr");
			conf.SetJobPriority(JobPriority.High);
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			FileInputFormat.SetInputPaths(conf, GetInputDir());
			FileOutputFormat.SetOutputPath(conf, GetOutputDir());
			return JobClient.RunJob(conf).GetID().ToString();
		}

		/// <exception cref="System.Exception"/>
		public static int RunTool(Configuration conf, Tool tool, string[] args, OutputStream
			 @out)
		{
			return TestMRJobClient.RunTool(conf, tool, args, @out);
		}

		/// <exception cref="System.Exception"/>
		internal static void VerifyJobPriority(string jobId, string priority, JobConf conf
			)
		{
			TestMRCJCJobClient test = new TestMRCJCJobClient();
			test.VerifyJobPriority(jobId, priority, conf, test.CreateJobClient());
		}

		/// <exception cref="System.Exception"/>
		public override void TestJobClient()
		{
			Configuration conf = CreateJobConf();
			string jobId = RunJob();
			TestGetCounter(jobId, conf);
			TestAllJobList(jobId, conf);
			TestChangingJobPriority(jobId, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override CLI CreateJobClient()
		{
			return new JobClient();
		}
	}
}
