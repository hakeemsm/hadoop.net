using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	/// <summary>class for testing transport of keys via Credentials .</summary>
	/// <remarks>
	/// class for testing transport of keys via Credentials .
	/// Client passes a list of keys in the Credentials object.
	/// The mapper and reducer checks whether it can access the keys
	/// from Credentials.
	/// </remarks>
	public class CredentialsTestJob : Configured, Tool
	{
		private const int NumOfKeys = 10;

		private static void CheckSecrets(Credentials ts)
		{
			if (ts == null)
			{
				throw new RuntimeException("The credentials are not available");
			}
			// fail the test
			for (int i = 0; i < NumOfKeys; i++)
			{
				string secretName = "alias" + i;
				// get token storage and a key
				byte[] secretValue = ts.GetSecretKey(new Text(secretName));
				System.Console.Out.WriteLine(secretValue);
				if (secretValue == null)
				{
					throw new RuntimeException("The key " + secretName + " is not available. ");
				}
				// fail the test
				string secretValueStr = Sharpen.Runtime.GetStringForBytes(secretValue);
				if (!("password" + i).Equals(secretValueStr))
				{
					throw new RuntimeException("The key " + secretName + " is not correct. Expected value is "
						 + ("password" + i) + ". Actual value is " + secretValueStr);
				}
			}
		}

		public class CredentialsTestMapper : Mapper<IntWritable, IntWritable, IntWritable
			, NullWritable>
		{
			internal Credentials ts;

			// fail the test
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				ts = context.GetCredentials();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, IntWritable value, Mapper.Context context
				)
			{
				CheckSecrets(ts);
			}
		}

		public class CredentialsTestReducer : Reducer<IntWritable, NullWritable, NullWritable
			, NullWritable>
		{
			internal Credentials ts;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Reducer.Context context)
			{
				ts = context.GetCredentials();
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Reduce(IntWritable key, IEnumerable<NullWritable> values, 
				Reducer.Context context)
			{
				CheckSecrets(ts);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new CredentialsTestJob(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Job CreateJob()
		{
			Configuration conf = GetConf();
			conf.SetInt(MRJobConfig.NumMaps, 1);
			Job job = Job.GetInstance(conf, "test");
			job.SetNumReduceTasks(1);
			job.SetJarByClass(typeof(CredentialsTestJob));
			job.SetNumReduceTasks(1);
			job.SetMapperClass(typeof(CredentialsTestJob.CredentialsTestMapper));
			job.SetMapOutputKeyClass(typeof(IntWritable));
			job.SetMapOutputValueClass(typeof(NullWritable));
			job.SetReducerClass(typeof(CredentialsTestJob.CredentialsTestReducer));
			job.SetInputFormatClass(typeof(SleepJob.SleepInputFormat));
			job.SetPartitionerClass(typeof(SleepJob.SleepJobPartitioner));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			job.SetSpeculativeExecution(false);
			job.SetJobName("test job");
			FileInputFormat.AddInputPath(job, new Path("ignored"));
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Job job = CreateJob();
			return job.WaitForCompletion(true) ? 0 : 1;
		}
	}
}
