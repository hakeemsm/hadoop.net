using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileComparators : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BLOCK_SIZE = 512;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FSDataOutputStream @out;

		private org.apache.hadoop.io.file.tfile.TFile.Writer writer;

		private string compression = org.apache.hadoop.io.file.tfile.Compression.Algorithm
			.GZ.getName();

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
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile);
			fs = path.getFileSystem(conf);
			@out = fs.create(path);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			fs.delete(path, true);
		}

		// bad comparator format
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureBadComparatorNames()
		{
			try
			{
				writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
					, "badcmp", conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (System.Exception e)
			{
				// noop, expecting exceptions
				Sharpen.Runtime.printStackTrace(e);
			}
		}

		// jclass that doesn't exist
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureBadJClassNames()
		{
			try
			{
				writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
					, "jclass: some.non.existence.clazz", conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (System.Exception e)
			{
				// noop, expecting exceptions
				Sharpen.Runtime.printStackTrace(e);
			}
		}

		// class exists but not a RawComparator
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureBadJClasses()
		{
			try
			{
				writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
					, "jclass:org.apache.hadoop.io.file.tfile.Chunk", conf);
				NUnit.Framework.Assert.Fail("Failed to catch unsupported comparator names");
			}
			catch (System.Exception e)
			{
				// noop, expecting exceptions
				Sharpen.Runtime.printStackTrace(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void closeOutput()
		{
			if (writer != null)
			{
				writer.close();
				writer = null;
			}
			if (@out != null)
			{
				@out.close();
				@out = null;
			}
		}
	}
}
