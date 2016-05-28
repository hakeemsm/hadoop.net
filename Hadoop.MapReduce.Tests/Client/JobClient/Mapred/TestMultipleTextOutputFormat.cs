using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMultipleTextOutputFormat : TestCase
	{
		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestMultipleTextOutputFormat()
		{
			try
			{
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static string attempt = "attempt_200707121733_0001_m_000000_0";

		private static Path workDir = new Path(new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), FileOutputCommitter.TempDirName), "_" + attempt);

		// A random task attempt id for testing.
		/// <exception cref="System.IO.IOException"/>
		private static void WriteData(RecordWriter<Text, Text> rw)
		{
			for (int i = 10; i < 40; i++)
			{
				string k = string.Empty + i;
				string v = string.Empty + i;
				rw.Write(new Text(k), new Text(v));
			}
		}

		internal class KeyBasedMultipleTextOutputFormat : MultipleTextOutputFormat<Text, 
			Text>
		{
			protected override string GenerateFileNameForKeyValue(Text key, Text v, string name
				)
			{
				return Sharpen.Runtime.Substring(key.ToString(), 0, 1) + "-" + name;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Test1(JobConf job)
		{
			FileSystem fs = FileSystem.GetLocal(job);
			string name = "part-00000";
			TestMultipleTextOutputFormat.KeyBasedMultipleTextOutputFormat theOutputFormat = new 
				TestMultipleTextOutputFormat.KeyBasedMultipleTextOutputFormat();
			RecordWriter<Text, Text> rw = theOutputFormat.GetRecordWriter(fs, job, name, null
				);
			WriteData(rw);
			rw.Close(null);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Test2(JobConf job)
		{
			FileSystem fs = FileSystem.GetLocal(job);
			string name = "part-00000";
			//pretend that we have input file with 1/2/3 as the suffix
			job.Set(JobContext.MapInputFile, "1/2/3");
			// we use the last two legs of the input file as the output file
			job.Set("mapred.outputformat.numOfTrailingLegs", "2");
			MultipleTextOutputFormat<Text, Text> theOutputFormat = new MultipleTextOutputFormat
				<Text, Text>();
			RecordWriter<Text, Text> rw = theOutputFormat.GetRecordWriter(fs, job, name, null
				);
			WriteData(rw);
			rw.Close(null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf();
			job.Set(JobContext.TaskAttemptId, attempt);
			FileOutputFormat.SetOutputPath(job, workDir.GetParent().GetParent());
			FileOutputFormat.SetWorkOutputPath(job, workDir);
			FileSystem fs = workDir.GetFileSystem(job);
			if (!fs.Mkdirs(workDir))
			{
				Fail("Failed to create output directory");
			}
			//System.out.printf("workdir: %s\n", workDir.toString());
			TestMultipleTextOutputFormat.Test1(job);
			TestMultipleTextOutputFormat.Test2(job);
			string file_11 = "1-part-00000";
			FilePath expectedFile_11 = new FilePath(new Path(workDir, file_11).ToString());
			//System.out.printf("expectedFile_11: %s\n", new Path(workDir, file_11).toString());
			StringBuilder expectedOutput = new StringBuilder();
			for (int i = 10; i < 20; i++)
			{
				expectedOutput.Append(string.Empty + i).Append('\t').Append(string.Empty + i).Append
					("\n");
			}
			string output = UtilsForTests.Slurp(expectedFile_11);
			//System.out.printf("File_2 output: %s\n", output);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
			string file_12 = "2-part-00000";
			FilePath expectedFile_12 = new FilePath(new Path(workDir, file_12).ToString());
			//System.out.printf("expectedFile_12: %s\n", new Path(workDir, file_12).toString());
			expectedOutput = new StringBuilder();
			for (int i_1 = 20; i_1 < 30; i_1++)
			{
				expectedOutput.Append(string.Empty + i_1).Append('\t').Append(string.Empty + i_1)
					.Append("\n");
			}
			output = UtilsForTests.Slurp(expectedFile_12);
			//System.out.printf("File_2 output: %s\n", output);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
			string file_13 = "3-part-00000";
			FilePath expectedFile_13 = new FilePath(new Path(workDir, file_13).ToString());
			//System.out.printf("expectedFile_13: %s\n", new Path(workDir, file_13).toString());
			expectedOutput = new StringBuilder();
			for (int i_2 = 30; i_2 < 40; i_2++)
			{
				expectedOutput.Append(string.Empty + i_2).Append('\t').Append(string.Empty + i_2)
					.Append("\n");
			}
			output = UtilsForTests.Slurp(expectedFile_13);
			//System.out.printf("File_2 output: %s\n", output);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
			string file_2 = "2/3";
			FilePath expectedFile_2 = new FilePath(new Path(workDir, file_2).ToString());
			//System.out.printf("expectedFile_2: %s\n", new Path(workDir, file_2).toString());
			expectedOutput = new StringBuilder();
			for (int i_3 = 10; i_3 < 40; i_3++)
			{
				expectedOutput.Append(string.Empty + i_3).Append('\t').Append(string.Empty + i_3)
					.Append("\n");
			}
			output = UtilsForTests.Slurp(expectedFile_2);
			//System.out.printf("File_2 output: %s\n", output);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestMultipleTextOutputFormat().TestFormat();
		}
	}
}
