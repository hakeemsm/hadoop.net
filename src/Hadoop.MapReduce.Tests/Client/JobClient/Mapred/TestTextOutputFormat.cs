using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTextOutputFormat
	{
		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestTextOutputFormat()
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
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormat()
		{
			JobConf job = new JobConf();
			job.Set(JobContext.TaskAttemptId, attempt);
			FileOutputFormat.SetOutputPath(job, workDir.GetParent().GetParent());
			FileOutputFormat.SetWorkOutputPath(job, workDir);
			FileSystem fs = workDir.GetFileSystem(job);
			if (!fs.Mkdirs(workDir))
			{
				NUnit.Framework.Assert.Fail("Failed to create output directory");
			}
			string file = "test_format.txt";
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat<object, object
				>();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(localFs
				, job, file, reporter);
			Text key1 = new Text("key1");
			Text key2 = new Text("key2");
			Text val1 = new Text("val1");
			Text val2 = new Text("val2");
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key1, val1);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val1);
				theRecordWriter.Write(nullWritable, val2);
				theRecordWriter.Write(key2, nullWritable);
				theRecordWriter.Write(key1, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key2, val2);
			}
			finally
			{
				theRecordWriter.Close(reporter);
			}
			FilePath expectedFile = new FilePath(new Path(workDir, file).ToString());
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append('\t').Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append('\t').Append(val2).Append("\n");
			string output = UtilsForTests.Slurp(expectedFile);
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), output);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithCustomSeparator()
		{
			JobConf job = new JobConf();
			string separator = "\u0001";
			job.Set("mapreduce.output.textoutputformat.separator", separator);
			job.Set(JobContext.TaskAttemptId, attempt);
			FileOutputFormat.SetOutputPath(job, workDir.GetParent().GetParent());
			FileOutputFormat.SetWorkOutputPath(job, workDir);
			FileSystem fs = workDir.GetFileSystem(job);
			if (!fs.Mkdirs(workDir))
			{
				NUnit.Framework.Assert.Fail("Failed to create output directory");
			}
			string file = "test_custom.txt";
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat<object, object
				>();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(localFs
				, job, file, reporter);
			Org.Apache.Hadoop.IO.Text key1 = new Org.Apache.Hadoop.IO.Text("key1");
			Org.Apache.Hadoop.IO.Text key2 = new Org.Apache.Hadoop.IO.Text("key2");
			Org.Apache.Hadoop.IO.Text val1 = new Org.Apache.Hadoop.IO.Text("val1");
			Org.Apache.Hadoop.IO.Text val2 = new Org.Apache.Hadoop.IO.Text("val2");
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key1, val1);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val1);
				theRecordWriter.Write(nullWritable, val2);
				theRecordWriter.Write(key2, nullWritable);
				theRecordWriter.Write(key1, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key2, val2);
			}
			finally
			{
				theRecordWriter.Close(reporter);
			}
			FilePath expectedFile = new FilePath(new Path(workDir, file).ToString());
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append(separator).Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append(separator).Append(val2).Append("\n");
			string output = UtilsForTests.Slurp(expectedFile);
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), output);
		}

		/// <summary>test compressed file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCompress()
		{
			JobConf job = new JobConf();
			job.Set(JobContext.TaskAttemptId, attempt);
			job.Set(FileOutputFormat.Compress, "true");
			FileOutputFormat.SetOutputPath(job, workDir.GetParent().GetParent());
			FileOutputFormat.SetWorkOutputPath(job, workDir);
			FileSystem fs = workDir.GetFileSystem(job);
			if (!fs.Mkdirs(workDir))
			{
				NUnit.Framework.Assert.Fail("Failed to create output directory");
			}
			string file = "test_compress.txt";
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat<object, object
				>();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(localFs
				, job, file, reporter);
			Org.Apache.Hadoop.IO.Text key1 = new Org.Apache.Hadoop.IO.Text("key1");
			Org.Apache.Hadoop.IO.Text key2 = new Org.Apache.Hadoop.IO.Text("key2");
			Org.Apache.Hadoop.IO.Text val1 = new Org.Apache.Hadoop.IO.Text("val1");
			Org.Apache.Hadoop.IO.Text val2 = new Org.Apache.Hadoop.IO.Text("val2");
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key1, val1);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val1);
				theRecordWriter.Write(nullWritable, val2);
				theRecordWriter.Write(key2, nullWritable);
				theRecordWriter.Write(key1, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key2, val2);
			}
			finally
			{
				theRecordWriter.Close(reporter);
			}
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append("\t").Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append("\t").Append(val2).Append("\n");
			DefaultCodec codec = new DefaultCodec();
			codec.SetConf(job);
			Path expectedFile = new Path(workDir, file + codec.GetDefaultExtension());
			FileInputStream istream = new FileInputStream(expectedFile.ToString());
			CompressionInputStream cistream = codec.CreateInputStream(istream);
			LineReader reader = new LineReader(cistream);
			string output = string.Empty;
			Org.Apache.Hadoop.IO.Text @out = new Org.Apache.Hadoop.IO.Text();
			while (reader.ReadLine(@out) > 0)
			{
				output += @out;
				output += "\n";
			}
			reader.Close();
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), output);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestTextOutputFormat().TestFormat();
		}
	}
}
