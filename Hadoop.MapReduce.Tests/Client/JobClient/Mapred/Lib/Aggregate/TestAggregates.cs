using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	public class TestAggregates : TestCase
	{
		private static NumberFormat idFormat = NumberFormat.GetInstance();

		static TestAggregates()
		{
			idFormat.SetMinimumIntegerDigits(4);
			idFormat.SetGroupingUsed(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAggregates()
		{
			Launch();
		}

		/// <exception cref="System.Exception"/>
		public static void Launch()
		{
			JobConf conf = new JobConf(typeof(Org.Apache.Hadoop.Mapred.Lib.Aggregate.TestAggregates
				));
			FileSystem fs = FileSystem.Get(conf);
			int numOfInputLines = 20;
			Path OutputDir = new Path("build/test/output_for_aggregates_test");
			Path InputDir = new Path("build/test/input_for_aggregates_test");
			string inputFile = "input.txt";
			fs.Delete(InputDir, true);
			fs.Mkdirs(InputDir);
			fs.Delete(OutputDir, true);
			StringBuilder inputData = new StringBuilder();
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append("max\t19\n");
			expectedOutput.Append("min\t1\n");
			FSDataOutputStream fileOut = fs.Create(new Path(InputDir, inputFile));
			for (int i = 1; i < numOfInputLines; i++)
			{
				expectedOutput.Append("count_").Append(idFormat.Format(i));
				expectedOutput.Append("\t").Append(i).Append("\n");
				inputData.Append(idFormat.Format(i));
				for (int j = 1; j < i; j++)
				{
					inputData.Append(" ").Append(idFormat.Format(i));
				}
				inputData.Append("\n");
			}
			expectedOutput.Append("value_as_string_max\t9\n");
			expectedOutput.Append("value_as_string_min\t1\n");
			expectedOutput.Append("uniq_count\t15\n");
			fileOut.Write(Sharpen.Runtime.GetBytesForString(inputData.ToString(), "utf-8"));
			fileOut.Close();
			System.Console.Out.WriteLine("inputData:");
			System.Console.Out.WriteLine(inputData.ToString());
			JobConf job = new JobConf(conf, typeof(Org.Apache.Hadoop.Mapred.Lib.Aggregate.TestAggregates
				));
			FileInputFormat.SetInputPaths(job, InputDir);
			job.SetInputFormat(typeof(TextInputFormat));
			FileOutputFormat.SetOutputPath(job, OutputDir);
			job.SetOutputFormat(typeof(TextOutputFormat));
			job.SetMapOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetMapOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetNumReduceTasks(1);
			job.SetMapperClass(typeof(ValueAggregatorMapper));
			job.SetReducerClass(typeof(ValueAggregatorReducer));
			job.SetCombinerClass(typeof(ValueAggregatorCombiner));
			job.SetInt("aggregator.descriptor.num", 1);
			job.Set("aggregator.descriptor.0", "UserDefined,org.apache.hadoop.mapred.lib.aggregate.AggregatorTests"
				);
			job.SetLong("aggregate.max.num.unique.values", 14);
			JobClient.RunJob(job);
			//
			// Finally, we compare the reconstructed answer key with the
			// original one.  Remember, we need to ignore zero-count items
			// in the original key.
			//
			bool success = true;
			Path outPath = new Path(OutputDir, "part-00000");
			string outdata = MapReduceTestUtil.ReadOutput(outPath, job);
			System.Console.Out.WriteLine("full out data:");
			System.Console.Out.WriteLine(outdata.ToString());
			outdata = Sharpen.Runtime.Substring(outdata, 0, expectedOutput.ToString().Length);
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), outdata);
			//fs.delete(OUTPUT_DIR);
			fs.Delete(InputDir, true);
		}

		/// <summary>Launches all the tasks in order.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			Launch();
		}
	}
}
