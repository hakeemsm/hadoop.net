using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>
	/// This class tests the logic for displaying the binary formats supported
	/// by the Text command.
	/// </summary>
	public class TestTextCommand
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data/") + "/testText";

		private static readonly string AvroFilename = new Path(TestRootDir, "weather.avro"
			).ToUri().GetPath();

		private static readonly string TextFilename = new Path(TestRootDir, "testtextfile.txt"
			).ToUri().GetPath();

		/// <summary>Tests whether binary Avro data files are displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDisplayForAvroFiles()
		{
			string expectedOutput = "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619506000000,\"temp\":22}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619484400000,\"temp\":-11}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655531200000,\"temp\":111}"
				 + Runtime.GetProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655509600000,\"temp\":78}"
				 + Runtime.GetProperty("line.separator");
			string output = ReadUsingTextCommand(AvroFilename, GenerateWeatherAvroBinaryData(
				));
			NUnit.Framework.Assert.AreEqual(expectedOutput, output);
		}

		/// <summary>Tests that a zero-length file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyTextFil()
		{
			byte[] emptyContents = new byte[] {  };
			string output = ReadUsingTextCommand(TextFilename, emptyContents);
			NUnit.Framework.Assert.IsTrue(string.Empty.Equals(output));
		}

		/// <summary>Tests that a one-byte file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestOneByteTextFil()
		{
			byte[] oneByteContents = new byte[] { (byte)('x') };
			string output = ReadUsingTextCommand(TextFilename, oneByteContents);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.GetStringForBytes(oneByteContents).
				Equals(output));
		}

		/// <summary>Tests that a one-byte file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTwoByteTextFil()
		{
			byte[] twoByteContents = new byte[] { (byte)('x'), (byte)('y') };
			string output = ReadUsingTextCommand(TextFilename, twoByteContents);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.GetStringForBytes(twoByteContents).
				Equals(output));
		}

		// Create a file on the local file system and read it using
		// the Display.Text class.
		/// <exception cref="System.Exception"/>
		private string ReadUsingTextCommand(string fileName, byte[] fileContents)
		{
			CreateFile(fileName, fileContents);
			// Prepare and call the Text command's protected getInputStream method
			// using reflection.
			Configuration conf = new Configuration();
			URI localPath = new URI(fileName);
			PathData pathData = new PathData(localPath, conf);
			Display.Text text = new _Text_111();
			text.SetConf(conf);
			InputStream stream = (InputStream)text.GetInputStream(pathData);
			return InputStreamToString(stream);
		}

		private sealed class _Text_111 : Display.Text
		{
			public _Text_111()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override InputStream GetInputStream(PathData item)
			{
				return base.GetInputStream(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string InputStreamToString(InputStream stream)
		{
			StringWriter writer = new StringWriter();
			IOUtils.Copy(stream, writer);
			return writer.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(string fileName, byte[] contents)
		{
			(new FilePath(TestRootDir)).Mkdir();
			FilePath file = new FilePath(fileName);
			file.CreateNewFile();
			FileOutputStream stream = new FileOutputStream(file);
			stream.Write(contents);
			stream.Close();
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
