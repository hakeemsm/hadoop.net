using System.IO;
using System.Reflection;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>
	/// This class tests the logic for displaying the binary formats supported
	/// by the Text command.
	/// </summary>
	public class TestHdfsTextCommand
	{
		private const string TestRootDir = "/build/test/data/testText";

		private static readonly Path AvroFilename = new Path(TestRootDir, "weather.avro");

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Tests whether binary Avro data files are displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDisplayForAvroFiles()
		{
			// Create a small Avro data file on the HDFS.
			CreateAvroFile(GenerateWeatherAvroBinaryData());
			// Prepare and call the Text command's protected getInputStream method
			// using reflection.
			Configuration conf = fs.GetConf();
			PathData pathData = new PathData(AvroFilename.ToString(), conf);
			Display.Text text = new Display.Text();
			text.SetConf(conf);
			MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(text.GetType(), "getInputStream"
				, typeof(PathData));
			InputStream stream = (InputStream)method.Invoke(text, pathData);
			string output = InputStreamToString(stream);
			// Check the output.
			string expectedOutput = "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619506000000,\"temp\":22}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619484400000,\"temp\":-11}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655531200000,\"temp\":111}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655509600000,\"temp\":78}"
				 + Runtime.GetProperty("line.separator");
			NUnit.Framework.Assert.AreEqual(expectedOutput, output);
		}

		/// <exception cref="System.IO.IOException"/>
		private string InputStreamToString(InputStream stream)
		{
			StringWriter writer = new StringWriter();
			IOUtils.Copy(stream, writer);
			return writer.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateAvroFile(byte[] contents)
		{
			FSDataOutputStream stream = fs.Create(AvroFilename);
			stream.Write(contents);
			stream.Close();
			NUnit.Framework.Assert.IsTrue(fs.Exists(AvroFilename));
		}

		private byte[] GenerateWeatherAvroBinaryData()
		{
			// The contents of a simple binary Avro file with weather records.
			byte[] contents = new byte[] { unchecked((byte)unchecked((int)(0x4f))), unchecked(
				(byte)unchecked((int)(0x62))), unchecked((byte)unchecked((int)(0x6a))), unchecked(
				(byte)unchecked((int)(0x1))), unchecked((byte)unchecked((int)(0x4))), unchecked(
				(byte)unchecked((int)(0x14))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x76))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x6f))), unchecked((byte)unchecked((int)(0x2e))), unchecked(
				(byte)unchecked((int)(0x63))), unchecked((byte)unchecked((int)(0x6f))), unchecked(
				(byte)unchecked((int)(0x64))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x63))), unchecked((byte)unchecked((int)(0x8))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x75))), unchecked(
				(byte)unchecked((int)(0x6c))), unchecked((byte)unchecked((int)(0x6c))), unchecked(
				(byte)unchecked((int)(0x16))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x76))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x6f))), unchecked((byte)unchecked((int)(0x2e))), unchecked(
				(byte)unchecked((int)(0x73))), unchecked((byte)unchecked((int)(0x63))), unchecked(
				(byte)unchecked((int)(0x68))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x6d))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0xf2))), unchecked((byte)unchecked((int)(0x2))), unchecked(
				(byte)unchecked((int)(0x7b))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x79))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x63))), unchecked(
				(byte)unchecked((int)(0x6f))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x64))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x6d))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x57))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x68))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x2c))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x6e))), unchecked(
				(byte)unchecked((int)(0x61))), unchecked((byte)unchecked((int)(0x6d))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x73))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x63))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x73))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x66))), unchecked((byte)unchecked((int)(0x69))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x6c))), unchecked(
				(byte)unchecked((int)(0x64))), unchecked((byte)unchecked((int)(0x73))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x5b))), unchecked((byte)unchecked((int)(0x7b))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x6e))), unchecked(
				(byte)unchecked((int)(0x61))), unchecked((byte)unchecked((int)(0x6d))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x3a))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x73))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x61))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x69))), unchecked((byte)unchecked((int)(0x6f))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x79))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x73))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x69))), unchecked((byte)unchecked((int)(0x6e))), unchecked(
				(byte)unchecked((int)(0x67))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x7d))), unchecked((byte)unchecked((int)(0x2c))), unchecked(
				(byte)unchecked((int)(0x7b))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x6d))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x69))), unchecked((byte)unchecked((int)(0x6d))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x79))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x6c))), unchecked(
				(byte)unchecked((int)(0x6f))), unchecked((byte)unchecked((int)(0x6e))), unchecked(
				(byte)unchecked((int)(0x67))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x7d))), unchecked((byte)unchecked((int)(0x2c))), unchecked(
				(byte)unchecked((int)(0x7b))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x6d))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x6d))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x79))), unchecked(
				(byte)unchecked((int)(0x70))), unchecked((byte)unchecked((int)(0x65))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x69))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x74))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x7d))), unchecked(
				(byte)unchecked((int)(0x5d))), unchecked((byte)unchecked((int)(0x2c))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x64))), unchecked(
				(byte)unchecked((int)(0x6f))), unchecked((byte)unchecked((int)(0x63))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x3a))), unchecked(
				(byte)unchecked((int)(0x22))), unchecked((byte)unchecked((int)(0x41))), unchecked(
				(byte)unchecked((int)(0x20))), unchecked((byte)unchecked((int)(0x77))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x74))), unchecked((byte)unchecked((int)(0x68))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x20))), unchecked((byte)unchecked((int)(0x72))), unchecked(
				(byte)unchecked((int)(0x65))), unchecked((byte)unchecked((int)(0x61))), unchecked(
				(byte)unchecked((int)(0x64))), unchecked((byte)unchecked((int)(0x69))), unchecked(
				(byte)unchecked((int)(0x6e))), unchecked((byte)unchecked((int)(0x67))), unchecked(
				(byte)unchecked((int)(0x2e))), unchecked((byte)unchecked((int)(0x22))), unchecked(
				(byte)unchecked((int)(0x7d))), unchecked((byte)unchecked((int)(0x0))), unchecked(
				(byte)unchecked((int)(0xb0))), unchecked((byte)unchecked((int)(0x81))), unchecked(
				(byte)unchecked((int)(0xb3))), unchecked((byte)unchecked((int)(0xc4))), unchecked(
				(byte)unchecked((int)(0xa))), unchecked((byte)unchecked((int)(0xc))), unchecked(
				(byte)unchecked((int)(0xf6))), unchecked((byte)unchecked((int)(0x62))), unchecked(
				(byte)unchecked((int)(0xfa))), unchecked((byte)unchecked((int)(0xc9))), unchecked(
				(byte)unchecked((int)(0x38))), unchecked((byte)unchecked((int)(0xfd))), unchecked(
				(byte)unchecked((int)(0x7e))), unchecked((byte)unchecked((int)(0x52))), unchecked(
				(byte)unchecked((int)(0x0))), unchecked((byte)unchecked((int)(0xa7))), unchecked(
				(byte)unchecked((int)(0xa))), unchecked((byte)unchecked((int)(0xcc))), unchecked(
				(byte)unchecked((int)(0x1))), unchecked((byte)unchecked((int)(0x18))), unchecked(
				(byte)unchecked((int)(0x30))), unchecked((byte)unchecked((int)(0x31))), unchecked(
				(byte)unchecked((int)(0x31))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x30))), unchecked(
				(byte)unchecked((int)(0x2d))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0xa3))), unchecked(
				(byte)unchecked((int)(0x90))), unchecked((byte)unchecked((int)(0xe8))), unchecked(
				(byte)unchecked((int)(0x87))), unchecked((byte)unchecked((int)(0x24))), unchecked(
				(byte)unchecked((int)(0x0))), unchecked((byte)unchecked((int)(0x18))), unchecked(
				(byte)unchecked((int)(0x30))), unchecked((byte)unchecked((int)(0x31))), unchecked(
				(byte)unchecked((int)(0x31))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x30))), unchecked(
				(byte)unchecked((int)(0x2d))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0x81))), unchecked(
				(byte)unchecked((int)(0xfb))), unchecked((byte)unchecked((int)(0xd6))), unchecked(
				(byte)unchecked((int)(0x87))), unchecked((byte)unchecked((int)(0x24))), unchecked(
				(byte)unchecked((int)(0x2c))), unchecked((byte)unchecked((int)(0x18))), unchecked(
				(byte)unchecked((int)(0x30))), unchecked((byte)unchecked((int)(0x31))), unchecked(
				(byte)unchecked((int)(0x31))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x30))), unchecked(
				(byte)unchecked((int)(0x2d))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0xa5))), unchecked(
				(byte)unchecked((int)(0xae))), unchecked((byte)unchecked((int)(0xc2))), unchecked(
				(byte)unchecked((int)(0x87))), unchecked((byte)unchecked((int)(0x24))), unchecked(
				(byte)unchecked((int)(0x15))), unchecked((byte)unchecked((int)(0x18))), unchecked(
				(byte)unchecked((int)(0x30))), unchecked((byte)unchecked((int)(0x31))), unchecked(
				(byte)unchecked((int)(0x32))), unchecked((byte)unchecked((int)(0x36))), unchecked(
				(byte)unchecked((int)(0x35))), unchecked((byte)unchecked((int)(0x30))), unchecked(
				(byte)unchecked((int)(0x2d))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0xb7))), unchecked(
				(byte)unchecked((int)(0xa2))), unchecked((byte)unchecked((int)(0x8b))), unchecked(
				(byte)unchecked((int)(0x94))), unchecked((byte)unchecked((int)(0x26))), unchecked(
				(byte)unchecked((int)(0xde))), unchecked((byte)unchecked((int)(0x1))), unchecked(
				(byte)unchecked((int)(0x18))), unchecked((byte)unchecked((int)(0x30))), unchecked(
				(byte)unchecked((int)(0x31))), unchecked((byte)unchecked((int)(0x32))), unchecked(
				(byte)unchecked((int)(0x36))), unchecked((byte)unchecked((int)(0x35))), unchecked(
				(byte)unchecked((int)(0x30))), unchecked((byte)unchecked((int)(0x2d))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0x39))), unchecked(
				(byte)unchecked((int)(0x39))), unchecked((byte)unchecked((int)(0xff))), unchecked(
				(byte)unchecked((int)(0xdb))), unchecked((byte)unchecked((int)(0xd5))), unchecked(
				(byte)unchecked((int)(0xf6))), unchecked((byte)unchecked((int)(0x93))), unchecked(
				(byte)unchecked((int)(0x26))), unchecked((byte)unchecked((int)(0x9c))), unchecked(
				(byte)unchecked((int)(0x1))), unchecked((byte)unchecked((int)(0xb0))), unchecked(
				(byte)unchecked((int)(0x81))), unchecked((byte)unchecked((int)(0xb3))), unchecked(
				(byte)unchecked((int)(0xc4))), unchecked((byte)unchecked((int)(0xa))), unchecked(
				(byte)unchecked((int)(0xc))), unchecked((byte)unchecked((int)(0xf6))), unchecked(
				(byte)unchecked((int)(0x62))), unchecked((byte)unchecked((int)(0xfa))), unchecked(
				(byte)unchecked((int)(0xc9))), unchecked((byte)unchecked((int)(0x38))), unchecked(
				(byte)unchecked((int)(0xfd))), unchecked((byte)unchecked((int)(0x7e))), unchecked(
				(byte)unchecked((int)(0x52))), unchecked((byte)unchecked((int)(0x0))), unchecked(
				(byte)unchecked((int)(0xa7))) };
			return contents;
		}
	}
}
