using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel
{
	public class TestMRFieldSelection : TestCase
	{
		private static NumberFormat idFormat = NumberFormat.GetInstance();

		static TestMRFieldSelection()
		{
			idFormat.SetMinimumIntegerDigits(4);
			idFormat.SetGroupingUsed(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFieldSelection()
		{
			Launch();
		}

		private static Path testDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), "field");

		/// <exception cref="System.Exception"/>
		public static void Launch()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.Get(conf);
			int numOfInputLines = 10;
			Path outDir = new Path(testDir, "output_for_field_selection_test");
			Path inDir = new Path(testDir, "input_for_field_selection_test");
			StringBuilder inputData = new StringBuilder();
			StringBuilder expectedOutput = new StringBuilder();
			ConstructInputOutputData(inputData, expectedOutput, numOfInputLines);
			conf.Set(FieldSelectionHelper.DataFieldSeperator, "-");
			conf.Set(FieldSelectionHelper.MapOutputKeyValueSpec, "6,5,1-3:0-");
			conf.Set(FieldSelectionHelper.ReduceOutputKeyValueSpec, ":4,3,2,1,0,0-");
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, inputData.ToString
				());
			job.SetMapperClass(typeof(FieldSelectionMapper));
			job.SetReducerClass(typeof(FieldSelectionReducer));
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetNumReduceTasks(1);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job Failed!", job.IsSuccessful());
			//
			// Finally, we compare the reconstructed answer key with the
			// original one.  Remember, we need to ignore zero-count items
			// in the original key.
			//
			string outdata = MapReduceTestUtil.ReadOutput(outDir, conf);
			NUnit.Framework.Assert.AreEqual("Outputs doesnt match.", expectedOutput.ToString(
				), outdata);
			fs.Delete(outDir, true);
		}

		public static void ConstructInputOutputData(StringBuilder inputData, StringBuilder
			 expectedOutput, int numOfInputLines)
		{
			for (int i = 0; i < numOfInputLines; i++)
			{
				inputData.Append(idFormat.Format(i));
				inputData.Append("-").Append(idFormat.Format(i + 1));
				inputData.Append("-").Append(idFormat.Format(i + 2));
				inputData.Append("-").Append(idFormat.Format(i + 3));
				inputData.Append("-").Append(idFormat.Format(i + 4));
				inputData.Append("-").Append(idFormat.Format(i + 5));
				inputData.Append("-").Append(idFormat.Format(i + 6));
				inputData.Append("\n");
				expectedOutput.Append(idFormat.Format(i + 3));
				expectedOutput.Append("-").Append(idFormat.Format(i + 2));
				expectedOutput.Append("-").Append(idFormat.Format(i + 1));
				expectedOutput.Append("-").Append(idFormat.Format(i + 5));
				expectedOutput.Append("-").Append(idFormat.Format(i + 6));
				expectedOutput.Append("-").Append(idFormat.Format(i + 6));
				expectedOutput.Append("-").Append(idFormat.Format(i + 5));
				expectedOutput.Append("-").Append(idFormat.Format(i + 1));
				expectedOutput.Append("-").Append(idFormat.Format(i + 2));
				expectedOutput.Append("-").Append(idFormat.Format(i + 3));
				expectedOutput.Append("-").Append(idFormat.Format(i + 0));
				expectedOutput.Append("-").Append(idFormat.Format(i + 1));
				expectedOutput.Append("-").Append(idFormat.Format(i + 2));
				expectedOutput.Append("-").Append(idFormat.Format(i + 3));
				expectedOutput.Append("-").Append(idFormat.Format(i + 4));
				expectedOutput.Append("-").Append(idFormat.Format(i + 5));
				expectedOutput.Append("-").Append(idFormat.Format(i + 6));
				expectedOutput.Append("\n");
			}
			System.Console.Out.WriteLine("inputData:");
			System.Console.Out.WriteLine(inputData.ToString());
			System.Console.Out.WriteLine("ExpectedData:");
			System.Console.Out.WriteLine(expectedOutput.ToString());
		}

		/// <summary>Launches all the tasks in order.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			Launch();
		}
	}
}
