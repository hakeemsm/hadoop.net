using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestSequenceFileSerialization : TestCase
	{
		private Configuration conf;

		private FileSystem fs;

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			fs = FileSystem.GetLocal(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJavaSerialization()
		{
			Path file = new Path(Runtime.GetProperty("test.build.data", ".") + "/testseqser.seq"
				);
			fs.Delete(file, true);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(long
				), typeof(string));
			writer.Append(1L, "one");
			writer.Append(2L, "two");
			writer.Close();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			Assert.Equal(1L, reader.Next((object)null));
			Assert.Equal("one", reader.GetCurrentValue((object)null));
			Assert.Equal(2L, reader.Next((object)null));
			Assert.Equal("two", reader.GetCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.Next((object)null));
			reader.Close();
		}
	}
}
