using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileComparator2
	{
		private static readonly string ROOT = Sharpen.Runtime.getProperty("test.build.data"
			, "/tmp/tfile-test");

		private const string name = "test-tfile-comparator2";

		private const int BLOCK_SIZE = 512;

		private const string VALUE = "value";

		private static readonly string jClassLongWritableComparator = "jclass:" + Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.io.LongWritable.Comparator)).getName();

		private const long NENTRY = 10000;

		private static long cube(long n)
		{
			return n * n * n;
		}

		private static string buildValue(long i)
		{
			return string.format("%s-%d", VALUE, i);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSortedLongWritable()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(ROOT, name);
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			try
			{
				org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
					(@out, BLOCK_SIZE, "gz", jClassLongWritableComparator, conf);
				try
				{
					org.apache.hadoop.io.LongWritable key = new org.apache.hadoop.io.LongWritable(0);
					for (long i = 0; i < NENTRY; ++i)
					{
						key.set(cube(i - NENTRY / 2));
						java.io.DataOutputStream dos = writer.prepareAppendKey(-1);
						try
						{
							key.write(dos);
						}
						finally
						{
							dos.close();
						}
						dos = writer.prepareAppendValue(-1);
						try
						{
							dos.write(Sharpen.Runtime.getBytesForString(buildValue(i)));
						}
						finally
						{
							dos.close();
						}
					}
				}
				finally
				{
					writer.close();
				}
			}
			finally
			{
				@out.close();
			}
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
			try
			{
				org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
					(@in, fs.getFileStatus(path).getLen(), conf);
				try
				{
					org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
						();
					long i = 0;
					org.apache.hadoop.io.BytesWritable value = new org.apache.hadoop.io.BytesWritable
						();
					for (; !scanner.atEnd(); scanner.advance())
					{
						scanner.entry().getValue(value);
						NUnit.Framework.Assert.AreEqual(buildValue(i), Sharpen.Runtime.getStringForBytes(
							value.getBytes(), 0, value.getLength()));
						++i;
					}
				}
				finally
				{
					reader.close();
				}
			}
			finally
			{
				@in.close();
			}
		}
	}
}
