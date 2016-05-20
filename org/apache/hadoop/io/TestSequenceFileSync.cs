using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestSequenceFileSync
	{
		private const int NUMRECORDS = 2000;

		private const int RECORDSIZE = 80;

		private static readonly java.util.Random rand = new java.util.Random();

		private const string REC_FMT = "%d RECORDID %d : ";

		/// <exception cref="System.IO.IOException"/>
		private static void forOffset(org.apache.hadoop.io.SequenceFile.Reader reader, org.apache.hadoop.io.IntWritable
			 key, org.apache.hadoop.io.Text val, int iter, long off, int expectedRecord)
		{
			val.clear();
			reader.sync(off);
			reader.next(key, val);
			NUnit.Framework.Assert.AreEqual(key.get(), expectedRecord);
			string test = string.format(REC_FMT, expectedRecord, expectedRecord);
			NUnit.Framework.Assert.AreEqual("Invalid value " + val, 0, val.find(test, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLowSyncpoint()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", "/tmp"), "sequencefile.sync.test");
			org.apache.hadoop.io.IntWritable input = new org.apache.hadoop.io.IntWritable();
			org.apache.hadoop.io.Text val = new org.apache.hadoop.io.Text();
			org.apache.hadoop.io.SequenceFile.Writer writer = new org.apache.hadoop.io.SequenceFile.Writer
				(fs, conf, path, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
			try
			{
				writeSequenceFile(writer, NUMRECORDS);
				for (int i = 0; i < 5; i++)
				{
					org.apache.hadoop.io.SequenceFile.Reader reader;
					//try different SequenceFile.Reader constructors
					if (i % 2 == 0)
					{
						reader = new org.apache.hadoop.io.SequenceFile.Reader(fs, path, conf);
					}
					else
					{
						org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
						long length = fs.getFileStatus(path).getLen();
						int buffersize = conf.getInt("io.file.buffer.size", 4096);
						reader = new org.apache.hadoop.io.SequenceFile.Reader(@in, buffersize, 0L, length
							, conf);
					}
					try
					{
						forOffset(reader, input, val, i, 0, 0);
						forOffset(reader, input, val, i, 65, 0);
						forOffset(reader, input, val, i, 2000, 21);
						forOffset(reader, input, val, i, 0, 0);
					}
					finally
					{
						reader.close();
					}
				}
			}
			finally
			{
				fs.delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void writeSequenceFile(org.apache.hadoop.io.SequenceFile.Writer writer
			, int numRecords)
		{
			org.apache.hadoop.io.IntWritable key = new org.apache.hadoop.io.IntWritable();
			org.apache.hadoop.io.Text val = new org.apache.hadoop.io.Text();
			for (int numWritten = 0; numWritten < numRecords; ++numWritten)
			{
				key.set(numWritten);
				randomText(val, numWritten, RECORDSIZE);
				writer.append(key, val);
			}
			writer.close();
		}

		internal static void randomText(org.apache.hadoop.io.Text val, int id, int recordSize
			)
		{
			val.clear();
			java.lang.StringBuilder ret = new java.lang.StringBuilder(recordSize);
			ret.Append(string.format(REC_FMT, id, id));
			recordSize -= ret.Length;
			for (int i = 0; i < recordSize; ++i)
			{
				ret.Append(rand.nextInt(9));
			}
			val.set(ret.ToString());
		}
	}
}
