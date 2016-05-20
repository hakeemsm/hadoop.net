using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestSequenceFileSerialization : NUnit.Framework.TestCase
	{
		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.FileSystem fs;

		/// <exception cref="System.Exception"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			fs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void tearDown()
		{
			fs.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testJavaSerialization()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/testseqser.seq");
			fs.delete(file, true);
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(fs, conf, file, Sharpen.Runtime.getClassForType(typeof(long)), Sharpen.Runtime.getClassForType
				(typeof(string)));
			writer.append(1L, "one");
			writer.append(2L, "two");
			writer.close();
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, file, conf);
			NUnit.Framework.Assert.AreEqual(1L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("one", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.AreEqual(2L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("two", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.next((object)null));
			reader.close();
		}
	}
}
