using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFieldSelection : TestCase
	{
		private static NumberFormat idFormat = NumberFormat.GetInstance();

		static TestFieldSelection()
		{
			idFormat.SetMinimumIntegerDigits(4);
			idFormat.SetGroupingUsed(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFieldSelection()
		{
			Launch();
		}

		/// <exception cref="System.Exception"/>
		public static void Launch()
		{
			JobConf conf = new JobConf(typeof(Org.Apache.Hadoop.Mapred.TestFieldSelection));
			FileSystem fs = FileSystem.Get(conf);
			int numOfInputLines = 10;
			Path OutputDir = new Path("build/test/output_for_field_selection_test");
			Path InputDir = new Path("build/test/input_for_field_selection_test");
			string inputFile = "input.txt";
			fs.Delete(InputDir, true);
			fs.Mkdirs(InputDir);
			fs.Delete(OutputDir, true);
			StringBuilder inputData = new StringBuilder();
			StringBuilder expectedOutput = new StringBuilder();
			TestMRFieldSelection.ConstructInputOutputData(inputData, expectedOutput, numOfInputLines
				);
			FSDataOutputStream fileOut = fs.Create(new Path(InputDir, inputFile));
			fileOut.Write(Sharpen.Runtime.GetBytesForString(inputData.ToString(), "utf-8"));
			fileOut.Close();
			System.Console.Out.WriteLine("inputData:");
			System.Console.Out.WriteLine(inputData.ToString());
			JobConf job = new JobConf(conf, typeof(Org.Apache.Hadoop.Mapred.TestFieldSelection
				));
			FileInputFormat.SetInputPaths(job, InputDir);
			job.SetInputFormat(typeof(TextInputFormat));
			job.SetMapperClass(typeof(FieldSelectionMapReduce));
			job.SetReducerClass(typeof(FieldSelectionMapReduce));
			FileOutputFormat.SetOutputPath(job, OutputDir);
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputFormat(typeof(TextOutputFormat));
			job.SetNumReduceTasks(1);
			job.Set(FieldSelectionHelper.DataFieldSeperator, "-");
			job.Set(FieldSelectionHelper.MapOutputKeyValueSpec, "6,5,1-3:0-");
			job.Set(FieldSelectionHelper.ReduceOutputKeyValueSpec, ":4,3,2,1,0,0-");
			JobClient.RunJob(job);
			//
			// Finally, we compare the reconstructed answer key with the
			// original one.  Remember, we need to ignore zero-count items
			// in the original key.
			//
			bool success = true;
			Path outPath = new Path(OutputDir, "part-00000");
			string outdata = MapReduceTestUtil.ReadOutput(outPath, job);
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), outdata);
			fs.Delete(OutputDir, true);
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
