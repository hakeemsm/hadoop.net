using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	public class TestAvroFSInput : TestCase
	{
		private const string InputDir = "AvroFSInput";

		private Path GetInputPath()
		{
			string dataDir = Runtime.GetProperty("test.build.data");
			if (null == dataDir)
			{
				return new Path(InputDir);
			}
			else
			{
				return new Path(new Path(dataDir), InputDir);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAFSInput()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path dir = GetInputPath();
			if (!fs.Exists(dir))
			{
				fs.Mkdirs(dir);
			}
			Path filePath = new Path(dir, "foo");
			if (fs.Exists(filePath))
			{
				fs.Delete(filePath, false);
			}
			FSDataOutputStream ostream = fs.Create(filePath);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(ostream));
			w.Write("0123456789");
			w.Close();
			// Create the stream
			FileContext fc = FileContext.GetFileContext(conf);
			AvroFSInput avroFSIn = new AvroFSInput(fc, filePath);
			Assert.Equal(10, avroFSIn.Length());
			// Check initial position
			byte[] buf = new byte[1];
			Assert.Equal(0, avroFSIn.Tell());
			// Check a read from that position.
			avroFSIn.Read(buf, 0, 1);
			Assert.Equal(1, avroFSIn.Tell());
			Assert.Equal('0', (char)buf[0]);
			// Check a seek + read
			avroFSIn.Seek(4);
			Assert.Equal(4, avroFSIn.Tell());
			avroFSIn.Read(buf, 0, 1);
			Assert.Equal('4', (char)buf[0]);
			Assert.Equal(5, avroFSIn.Tell());
			avroFSIn.Close();
		}
	}
}
