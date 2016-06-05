using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileComparators : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BlockSize = 512;

		private FileSystem fs;

		private Configuration conf;

		private Path path;

		private FSDataOutputStream @out;

		private TFile.Writer writer;

		private string compression = Compression.Algorithm.Gz.GetName();

		private string outputFile = "TFileTestComparators";

		private int records1stBlock = 4480;

		private int records2ndBlock = 4263;

		/*
		* pre-sampled numbers of records in one block, based on the given the
		* generated key and value strings
		*/
		// private int records1stBlock = 4314;
		// private int records2ndBlock = 4108;
		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			path = new Path(Root, outputFile);
			fs = path.GetFileSystem(conf);
			@out = fs.Create(path);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			fs.Delete(path, true);
		}

		// bad comparator format
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureBadComparatorNames()
		{
			try
			{
				writer = new TFile.Writer(@out, BlockSize, compression, "badcmp", conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (Exception e)
			{
				// noop, expecting exceptions
				Runtime.PrintStackTrace(e);
			}
		}

		// jclass that doesn't exist
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureBadJClassNames()
		{
			try
			{
				writer = new TFile.Writer(@out, BlockSize, compression, "jclass: some.non.existence.clazz"
					, conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (Exception e)
			{
				// noop, expecting exceptions
				Runtime.PrintStackTrace(e);
			}
		}

		// class exists but not a RawComparator
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureBadJClasses()
		{
			try
			{
				writer = new TFile.Writer(@out, BlockSize, compression, "jclass:org.apache.hadoop.io.file.tfile.Chunk"
					, conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (Exception e)
			{
				// noop, expecting exceptions
				Runtime.PrintStackTrace(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CloseOutput()
		{
			if (writer != null)
			{
				writer.Close();
				writer = null;
			}
			if (@out != null)
			{
				@out.Close();
				@out = null;
			}
		}
	}
}
