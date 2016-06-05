using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileComparator2
	{
		private static readonly string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const string name = "test-tfile-comparator2";

		private const int BlockSize = 512;

		private const string Value = "value";

		private static readonly string jClassLongWritableComparator = "jclass:" + typeof(
			LongWritable.Comparator).FullName;

		private const long Nentry = 10000;

		private static long Cube(long n)
		{
			return n * n * n;
		}

		private static string BuildValue(long i)
		{
			return string.Format("%s-%d", Value, i);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSortedLongWritable()
		{
			Configuration conf = new Configuration();
			Path path = new Path(Root, name);
			FileSystem fs = path.GetFileSystem(conf);
			FSDataOutputStream @out = fs.Create(path);
			try
			{
				TFile.Writer writer = new TFile.Writer(@out, BlockSize, "gz", jClassLongWritableComparator
					, conf);
				try
				{
					LongWritable key = new LongWritable(0);
					for (long i = 0; i < Nentry; ++i)
					{
						key.Set(Cube(i - Nentry / 2));
						DataOutputStream dos = writer.PrepareAppendKey(-1);
						try
						{
							key.Write(dos);
						}
						finally
						{
							dos.Close();
						}
						dos = writer.PrepareAppendValue(-1);
						try
						{
							dos.Write(Runtime.GetBytesForString(BuildValue(i)));
						}
						finally
						{
							dos.Close();
						}
					}
				}
				finally
				{
					writer.Close();
				}
			}
			finally
			{
				@out.Close();
			}
			FSDataInputStream @in = fs.Open(path);
			try
			{
				TFile.Reader reader = new TFile.Reader(@in, fs.GetFileStatus(path).GetLen(), conf
					);
				try
				{
					TFile.Reader.Scanner scanner = reader.CreateScanner();
					long i = 0;
					BytesWritable value = new BytesWritable();
					for (; !scanner.AtEnd(); scanner.Advance())
					{
						scanner.Entry().GetValue(value);
						Assert.Equal(BuildValue(i), Runtime.GetStringForBytes(
							value.GetBytes(), 0, value.GetLength()));
						++i;
					}
				}
				finally
				{
					reader.Close();
				}
			}
			finally
			{
				@in.Close();
			}
		}
	}
}
