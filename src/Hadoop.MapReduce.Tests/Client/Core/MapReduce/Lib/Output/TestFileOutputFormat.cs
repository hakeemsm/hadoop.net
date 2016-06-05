using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestFileOutputFormat : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestSetOutputPathException()
		{
			Job job = Job.GetInstance();
			try
			{
				// Give it an invalid filesystem so it'll throw an exception
				FileOutputFormat.SetOutputPath(job, new Path("foo:///bar"));
				Fail("Should have thrown a RuntimeException with an IOException inside");
			}
			catch (RuntimeException re)
			{
				NUnit.Framework.Assert.IsTrue(re.InnerException is IOException);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckOutputSpecsException()
		{
			Job job = Job.GetInstance();
			Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"), "output");
			FileSystem fs = outDir.GetFileSystem(new Configuration());
			// Create the output dir so it already exists and set it for the job
			fs.Mkdirs(outDir);
			FileOutputFormat.SetOutputPath(job, outDir);
			// We don't need a "full" implementation of FileOutputFormat for this test
			FileOutputFormat fof = new _FileOutputFormat_54();
			try
			{
				try
				{
					// This should throw a FileAlreadyExistsException because the outputDir
					// already exists
					fof.CheckOutputSpecs(job);
					Fail("Should have thrown a FileAlreadyExistsException");
				}
				catch (FileAlreadyExistsException)
				{
				}
			}
			finally
			{
				// correct behavior
				// Cleanup
				if (fs.Exists(outDir))
				{
					fs.Delete(outDir, true);
				}
			}
		}

		private sealed class _FileOutputFormat_54 : FileOutputFormat
		{
			public _FileOutputFormat_54()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordWriter GetRecordWriter(TaskAttemptContext job)
			{
				return null;
			}
		}
	}
}
