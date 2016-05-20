using Sharpen;

namespace org.apache.hadoop.io.compress
{
	public class TestCompressorStream : org.apache.hadoop.io.compress.CompressorStream
	{
		private static java.io.FileOutputStream fop = null;

		private static java.io.File file = null;

		static TestCompressorStream()
		{
			try
			{
				file = new java.io.File("tmp.txt");
				fop = new java.io.FileOutputStream(file);
				if (!file.exists())
				{
					file.createNewFile();
				}
			}
			catch (System.IO.IOException e)
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
		/// <see cref="CompressorStream.finish()"/>
		/// method in order
		/// to reproduce test case
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void finish()
		{
			throw new System.IO.IOException();
		}

		/// <summary>
		/// In
		/// <see cref="CompressorStream.close()"/>
		/// , if
		/// <see cref="CompressorStream.finish()"/>
		/// throws an IOEXception, outputStream
		/// object was not getting closed.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testClose()
		{
			org.apache.hadoop.io.compress.TestCompressorStream testCompressorStream = new org.apache.hadoop.io.compress.TestCompressorStream
				();
			try
			{
				testCompressorStream.close();
			}
			catch (System.IO.IOException)
			{
				System.Console.Out.WriteLine("Expected IOException");
			}
			NUnit.Framework.Assert.IsTrue("closed shoud be true", ((org.apache.hadoop.io.compress.CompressorStream
				)testCompressorStream).closed);
			//cleanup after test case
			file.delete();
		}
	}
}
