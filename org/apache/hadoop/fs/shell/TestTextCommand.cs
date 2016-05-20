using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>
	/// This class tests the logic for displaying the binary formats supported
	/// by the Text command.
	/// </summary>
	public class TestTextCommand
	{
		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data/") + "/testText";

		private static readonly string AVRO_FILENAME = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR
			, "weather.avro").toUri().getPath();

		private static readonly string TEXT_FILENAME = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR
			, "testtextfile.txt").toUri().getPath();

		/// <summary>Tests whether binary Avro data files are displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testDisplayForAvroFiles()
		{
			string expectedOutput = "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}"
				 + Sharpen.Runtime.getProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619506000000,\"temp\":22}"
				 + Sharpen.Runtime.getProperty("line.separator") + "{\"station\":\"011990-99999\",\"time\":-619484400000,\"temp\":-11}"
				 + Sharpen.Runtime.getProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655531200000,\"temp\":111}"
				 + Sharpen.Runtime.getProperty("line.separator") + "{\"station\":\"012650-99999\",\"time\":-655509600000,\"temp\":78}"
				 + Sharpen.Runtime.getProperty("line.separator");
			string output = readUsingTextCommand(AVRO_FILENAME, generateWeatherAvroBinaryData
				());
			NUnit.Framework.Assert.AreEqual(expectedOutput, output);
		}

		/// <summary>Tests that a zero-length file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testEmptyTextFil()
		{
			byte[] emptyContents = new byte[] {  };
			string output = readUsingTextCommand(TEXT_FILENAME, emptyContents);
			NUnit.Framework.Assert.IsTrue(string.Empty.Equals(output));
		}

		/// <summary>Tests that a one-byte file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testOneByteTextFil()
		{
			byte[] oneByteContents = new byte[] { (byte)('x') };
			string output = readUsingTextCommand(TEXT_FILENAME, oneByteContents);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.getStringForBytes(oneByteContents).
				Equals(output));
		}

		/// <summary>Tests that a one-byte file is displayed correctly.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testTwoByteTextFil()
		{
			byte[] twoByteContents = new byte[] { (byte)('x'), (byte)('y') };
			string output = readUsingTextCommand(TEXT_FILENAME, twoByteContents);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.getStringForBytes(twoByteContents).
				Equals(output));
		}

		// Create a file on the local file system and read it using
		// the Display.Text class.
		/// <exception cref="System.Exception"/>
		private string readUsingTextCommand(string fileName, byte[] fileContents)
		{
			createFile(fileName, fileContents);
			// Prepare and call the Text command's protected getInputStream method
			// using reflection.
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.net.URI localPath = new java.net.URI(fileName);
			org.apache.hadoop.fs.shell.PathData pathData = new org.apache.hadoop.fs.shell.PathData
				(localPath, conf);
			org.apache.hadoop.fs.shell.Display.Text text = new _Text_111();
			text.setConf(conf);
			java.io.InputStream stream = (java.io.InputStream)text.getInputStream(pathData);
			return inputStreamToString(stream);
		}

		private sealed class _Text_111 : org.apache.hadoop.fs.shell.Display.Text
		{
			public _Text_111()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override java.io.InputStream getInputStream(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				return base.getInputStream(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string inputStreamToString(java.io.InputStream stream)
		{
			System.IO.StringWriter writer = new System.IO.StringWriter();
			org.apache.commons.io.IOUtils.copy(stream, writer);
			return writer.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void createFile(string fileName, byte[] contents)
		{
			(new java.io.File(TEST_ROOT_DIR)).mkdir();
			java.io.File file = new java.io.File(fileName);
			file.createNewFile();
			java.io.FileOutputStream stream = new java.io.FileOutputStream(file);
			stream.write(contents);
			stream.close();
		}

		private byte[] generateWeatherAvroBinaryData()
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
