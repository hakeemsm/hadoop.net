using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMultiFileInputFormat : TestCase
	{
		private static JobConf job = new JobConf();

		private static readonly Log Log = LogFactory.GetLog(typeof(TestMultiFileInputFormat
			));

		private const int MaxSplitCount = 10000;

		private const int SplitCountIncr = 6000;

		private const int MaxBytes = 1024;

		private const int MaxNumFiles = 10000;

		private const int NumFilesIncr = 8000;

		private Random rand = new Random(Runtime.CurrentTimeMillis());

		private Dictionary<string, long> lengths = new Dictionary<string, long>();

		/// <summary>Dummy class to extend MultiFileInputFormat</summary>
		private class DummyMultiFileInputFormat : MultiFileInputFormat<Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<Text, Text> GetRecordReader(InputSplit split, JobConf
				 job, Reporter reporter)
			{
				return null;
			}

			internal DummyMultiFileInputFormat(TestMultiFileInputFormat _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultiFileInputFormat _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path InitFiles(FileSystem fs, int numFiles, int numBytes)
		{
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path multiFileDir = new Path(dir, "test.multifile");
			fs.Delete(multiFileDir, true);
			fs.Mkdirs(multiFileDir);
			Log.Info("Creating " + numFiles + " file(s) in " + multiFileDir);
			for (int i = 0; i < numFiles; i++)
			{
				Path path = new Path(multiFileDir, "file_" + i);
				FSDataOutputStream @out = fs.Create(path);
				if (numBytes == -1)
				{
					numBytes = rand.Next(MaxBytes);
				}
				for (int j = 0; j < numBytes; j++)
				{
					@out.Write(rand.Next());
				}
				@out.Close();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Created file " + path + " with length " + numBytes);
				}
				lengths[path.GetName()] = System.Convert.ToInt64(numBytes);
			}
			FileInputFormat.SetInputPaths(job, multiFileDir);
			return multiFileDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFormat()
		{
			Log.Info("Test started");
			Log.Info("Max split count           = " + MaxSplitCount);
			Log.Info("Split count increment     = " + SplitCountIncr);
			Log.Info("Max bytes per file        = " + MaxBytes);
			Log.Info("Max number of files       = " + MaxNumFiles);
			Log.Info("Number of files increment = " + NumFilesIncr);
			MultiFileInputFormat<Text, Text> format = new TestMultiFileInputFormat.DummyMultiFileInputFormat
				(this);
			FileSystem fs = FileSystem.GetLocal(job);
			for (int numFiles = 1; numFiles < MaxNumFiles; numFiles += (NumFilesIncr / 2) + rand
				.Next(NumFilesIncr / 2))
			{
				Path dir = InitFiles(fs, numFiles, -1);
				BitSet bits = new BitSet(numFiles);
				for (int i = 1; i < MaxSplitCount; i += rand.Next(SplitCountIncr) + 1)
				{
					Log.Info("Running for Num Files=" + numFiles + ", split count=" + i);
					MultiFileSplit[] splits = (MultiFileSplit[])format.GetSplits(job, i);
					bits.Clear();
					foreach (MultiFileSplit split in splits)
					{
						long splitLength = 0;
						foreach (Path p in split.GetPaths())
						{
							long length = fs.GetContentSummary(p).GetLength();
							NUnit.Framework.Assert.AreEqual(length, lengths[p.GetName()]);
							splitLength += length;
							string name = p.GetName();
							int index = System.Convert.ToInt32(Sharpen.Runtime.Substring(name, name.LastIndexOf
								("file_") + 5));
							NUnit.Framework.Assert.IsFalse(bits.Get(index));
							bits.Set(index);
						}
						NUnit.Framework.Assert.AreEqual(splitLength, split.GetLength());
					}
				}
				NUnit.Framework.Assert.AreEqual(bits.Cardinality(), numFiles);
				fs.Delete(dir, true);
			}
			Log.Info("Test Finished");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFormatWithLessPathsThanSplits()
		{
			MultiFileInputFormat<Text, Text> format = new TestMultiFileInputFormat.DummyMultiFileInputFormat
				(this);
			FileSystem fs = FileSystem.GetLocal(job);
			// Test with no path
			InitFiles(fs, 0, -1);
			NUnit.Framework.Assert.AreEqual(0, format.GetSplits(job, 2).Length);
			// Test with 2 path and 4 splits
			InitFiles(fs, 2, 500);
			NUnit.Framework.Assert.AreEqual(2, format.GetSplits(job, 4).Length);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestMultiFileInputFormat test = new TestMultiFileInputFormat();
			test.TestFormat();
		}
	}
}
