using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestAvroFSInput : NUnit.Framework.TestCase
	{
		private const string INPUT_DIR = "AvroFSInput";

		private org.apache.hadoop.fs.Path getInputPath()
		{
			string dataDir = Sharpen.Runtime.getProperty("test.build.data");
			if (null == dataDir)
			{
				return new org.apache.hadoop.fs.Path(INPUT_DIR);
			}
			else
			{
				return new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(dataDir), INPUT_DIR
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAFSInput()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path dir = getInputPath();
			if (!fs.exists(dir))
			{
				fs.mkdirs(dir);
			}
			org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(dir, "foo");
			if (fs.exists(filePath))
			{
				fs.delete(filePath, false);
			}
			org.apache.hadoop.fs.FSDataOutputStream ostream = fs.create(filePath);
			java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.OutputStreamWriter
				(ostream));
			w.write("0123456789");
			w.close();
			// Create the stream
			org.apache.hadoop.fs.FileContext fc = org.apache.hadoop.fs.FileContext.getFileContext
				(conf);
			org.apache.hadoop.fs.AvroFSInput avroFSIn = new org.apache.hadoop.fs.AvroFSInput(
				fc, filePath);
			NUnit.Framework.Assert.AreEqual(10, avroFSIn.length());
			// Check initial position
			byte[] buf = new byte[1];
			NUnit.Framework.Assert.AreEqual(0, avroFSIn.tell());
			// Check a read from that position.
			avroFSIn.read(buf, 0, 1);
			NUnit.Framework.Assert.AreEqual(1, avroFSIn.tell());
			NUnit.Framework.Assert.AreEqual('0', (char)buf[0]);
			// Check a seek + read
			avroFSIn.seek(4);
			NUnit.Framework.Assert.AreEqual(4, avroFSIn.tell());
			avroFSIn.read(buf, 0, 1);
			NUnit.Framework.Assert.AreEqual('4', (char)buf[0]);
			NUnit.Framework.Assert.AreEqual(5, avroFSIn.tell());
			avroFSIn.close();
		}
	}
}
