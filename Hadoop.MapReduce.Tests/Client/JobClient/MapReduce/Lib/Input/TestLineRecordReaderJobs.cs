using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestLineRecordReaderJobs
	{
		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestTextInputFormat");

		private static Path inputDir = new Path(workDir, "input");

		private static Path outputDir = new Path(workDir, "output");

		/// <summary>Writes the input test file</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateInputFile(Configuration conf)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			Path file = new Path(inputDir, "test.txt");
			TextWriter writer = new OutputStreamWriter(localFs.Create(file));
			writer.Write("abc\ndef\t\nghi\njkl");
			writer.Close();
		}

		/// <summary>Reads the output file into a string</summary>
		/// <param name="conf"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadOutputFile(Configuration conf)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			Path file = new Path(outputDir, "part-r-00000");
			return UtilsForTests.SlurpHadoop(file, localFs);
		}

		/// <summary>Creates and runs an MR job</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void CreateAndRunJob(Configuration conf)
		{
			Job job = Job.GetInstance(conf);
			job.SetJarByClass(typeof(TestLineRecordReaderJobs));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(Reducer));
			FileInputFormat.AddInputPath(job, inputDir);
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.WaitForCompletion(true);
		}

		/// <summary>
		/// Test the case when a custom record delimiter is specified using the
		/// textinputformat.record.delimiter configuration property
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomRecordDelimiters()
		{
			Configuration conf = new Configuration();
			conf.Set("textinputformat.record.delimiter", "\t\n");
			FileSystem localFs = FileSystem.GetLocal(conf);
			// cleanup
			localFs.Delete(workDir, true);
			// creating input test file
			CreateInputFile(conf);
			CreateAndRunJob(conf);
			string expected = "0\tabc\ndef\n9\tghi\njkl\n";
			NUnit.Framework.Assert.AreEqual(expected, ReadOutputFile(conf));
		}

		/// <summary>
		/// Test the default behavior when the textinputformat.record.delimiter
		/// configuration property is not specified
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRecordDelimiters()
		{
			Configuration conf = new Configuration();
			FileSystem localFs = FileSystem.GetLocal(conf);
			// cleanup
			localFs.Delete(workDir, true);
			// creating input test file
			CreateInputFile(conf);
			CreateAndRunJob(conf);
			string expected = "0\tabc\n4\tdef\t\n9\tghi\n13\tjkl\n";
			NUnit.Framework.Assert.AreEqual(expected, ReadOutputFile(conf));
		}
	}
}
