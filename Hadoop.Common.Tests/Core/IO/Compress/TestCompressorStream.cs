using System.IO;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	public class TestCompressorStream : CompressorStream
	{
		private static FileOutputStream fop = null;

		private static FilePath file = null;

		static TestCompressorStream()
		{
			try
			{
				file = new FilePath("tmp.txt");
				fop = new FileOutputStream(file);
				if (!file.Exists())
				{
					file.CreateNewFile();
				}
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("Error while creating a new file " + e.Message);
			}
		}

		public TestCompressorStream()
			: base(fop)
		{
		}

		/// <summary>
		/// Overriding
		/// <see cref="CompressorStream.Finish()"/>
		/// method in order
		/// to reproduce test case
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Finish()
		{
			throw new IOException();
		}

		/// <summary>
		/// In
		/// <see cref="CompressorStream.Close()"/>
		/// , if
		/// <see cref="CompressorStream.Finish()"/>
		/// throws an IOEXception, outputStream
		/// object was not getting closed.
		/// </summary>
		[Fact]
		public virtual void TestClose()
		{
			Org.Apache.Hadoop.IO.Compress.TestCompressorStream testCompressorStream = new Org.Apache.Hadoop.IO.Compress.TestCompressorStream
				();
			try
			{
				testCompressorStream.Close();
			}
			catch (IOException)
			{
				System.Console.Out.WriteLine("Expected IOException");
			}
			Assert.True("closed shoud be true", ((CompressorStream)testCompressorStream
				).closed);
			//cleanup after test case
			file.Delete();
		}
	}
}
