using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestCombineFileInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCombineFileInputFormat
			).FullName);

		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestCombineFileInputFormat()
		{
			try
			{
				defaultConf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")), "TestCombineFileInputFormat").MakeQualified(localFs);

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path name, string contents)
		{
			OutputStream stm;
			stm = fs.Create(name);
			stm.Write(Sharpen.Runtime.GetBytesForString(contents));
			stm.Close();
		}

		/// <summary>Test getSplits</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSplits()
		{
			JobConf job = new JobConf(defaultConf);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "test.txt"), "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n"
				);
			FileInputFormat.SetInputPaths(job, workDir);
			CombineFileInputFormat format = new _CombineFileInputFormat_73();
			int SizeSplits = 1;
			Log.Info("Trying to getSplits with splits = " + SizeSplits);
			InputSplit[] splits = format.GetSplits(job, SizeSplits);
			Log.Info("Got getSplits = " + splits.Length);
			NUnit.Framework.Assert.AreEqual("splits == " + SizeSplits, SizeSplits, splits.Length
				);
		}

		private sealed class _CombineFileInputFormat_73 : CombineFileInputFormat
		{
			public _CombineFileInputFormat_73()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override RecordReader GetRecordReader(InputSplit split, JobConf job, Reporter
				 reporter)
			{
				return new CombineFileRecordReader(job, (CombineFileSplit)split, reporter, typeof(
					CombineFileRecordReader));
			}
		}
	}
}
