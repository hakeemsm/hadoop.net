using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestSequenceFileSync
	{
		private const int Numrecords = 2000;

		private const int Recordsize = 80;

		private static readonly Random rand = new Random();

		private const string RecFmt = "%d RECORDID %d : ";

		/// <exception cref="System.IO.IOException"/>
		private static void ForOffset(SequenceFile.Reader reader, IntWritable key, Text val
			, int iter, long off, int expectedRecord)
		{
			val.Clear();
			reader.Sync(off);
			reader.Next(key, val);
			NUnit.Framework.Assert.AreEqual(key.Get(), expectedRecord);
			string test = string.Format(RecFmt, expectedRecord, expectedRecord);
			NUnit.Framework.Assert.AreEqual("Invalid value " + val, 0, val.Find(test, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLowSyncpoint()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path path = new Path(Runtime.GetProperty("test.build.data", "/tmp"), "sequencefile.sync.test"
				);
			IntWritable input = new IntWritable();
			Text val = new Text();
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, typeof(IntWritable
				), typeof(Text));
			try
			{
				WriteSequenceFile(writer, Numrecords);
				for (int i = 0; i < 5; i++)
				{
					SequenceFile.Reader reader;
					//try different SequenceFile.Reader constructors
					if (i % 2 == 0)
					{
						reader = new SequenceFile.Reader(fs, path, conf);
					}
					else
					{
						FSDataInputStream @in = fs.Open(path);
						long length = fs.GetFileStatus(path).GetLen();
						int buffersize = conf.GetInt("io.file.buffer.size", 4096);
						reader = new SequenceFile.Reader(@in, buffersize, 0L, length, conf);
					}
					try
					{
						ForOffset(reader, input, val, i, 0, 0);
						ForOffset(reader, input, val, i, 65, 0);
						ForOffset(reader, input, val, i, 2000, 21);
						ForOffset(reader, input, val, i, 0, 0);
					}
					finally
					{
						reader.Close();
					}
				}
			}
			finally
			{
				fs.Delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteSequenceFile(SequenceFile.Writer writer, int numRecords)
		{
			IntWritable key = new IntWritable();
			Text val = new Text();
			for (int numWritten = 0; numWritten < numRecords; ++numWritten)
			{
				key.Set(numWritten);
				RandomText(val, numWritten, Recordsize);
				writer.Append(key, val);
			}
			writer.Close();
		}

		internal static void RandomText(Text val, int id, int recordSize)
		{
			val.Clear();
			StringBuilder ret = new StringBuilder(recordSize);
			ret.Append(string.Format(RecFmt, id, id));
			recordSize -= ret.Length;
			for (int i = 0; i < recordSize; ++i)
			{
				ret.Append(rand.Next(9));
			}
			val.Set(ret.ToString());
		}
	}
}
