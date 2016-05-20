using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Testing the correctness of FileSystem.getFileBlockLocations.</summary>
	public class TestGetFileBlockLocations : NUnit.Framework.TestCase
	{
		private static string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "/tmp/testGetFileBlockLocations");

		private const int FileLength = 4 * 1024 * 1024;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FileSystem fs;

		private java.util.Random random;

		// 4MB
		/// <seealso cref="NUnit.Framework.TestCase.setUp()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			org.apache.hadoop.fs.Path rootPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR);
			path = new org.apache.hadoop.fs.Path(rootPath, "TestGetFileBlockLocations");
			fs = rootPath.getFileSystem(conf);
			org.apache.hadoop.fs.FSDataOutputStream fsdos = fs.create(path, true);
			byte[] buffer = new byte[1024];
			while (fsdos.getPos() < FileLength)
			{
				fsdos.write(buffer);
			}
			fsdos.close();
			random = new java.util.Random(Sharpen.Runtime.nanoTime());
		}

		/// <exception cref="System.IO.IOException"/>
		private void oneTest(int offBegin, int offEnd, org.apache.hadoop.fs.FileStatus status
			)
		{
			if (offBegin > offEnd)
			{
				int tmp = offBegin;
				offBegin = offEnd;
				offEnd = tmp;
			}
			org.apache.hadoop.fs.BlockLocation[] locations = fs.getFileBlockLocations(status, 
				offBegin, offEnd - offBegin);
			if (offBegin < status.getLen())
			{
				java.util.Arrays.sort(locations, new _Comparator_69());
				offBegin = (int)System.Math.min(offBegin, status.getLen() - 1);
				offEnd = (int)System.Math.min(offEnd, status.getLen());
				org.apache.hadoop.fs.BlockLocation first = locations[0];
				org.apache.hadoop.fs.BlockLocation last = locations[locations.Length - 1];
				NUnit.Framework.Assert.IsTrue(first.getOffset() <= offBegin);
				NUnit.Framework.Assert.IsTrue(offEnd <= last.getOffset() + last.getLength());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(locations.Length == 0);
			}
		}

		private sealed class _Comparator_69 : java.util.Comparator<org.apache.hadoop.fs.BlockLocation
			>
		{
			public _Comparator_69()
			{
			}

			public int compare(org.apache.hadoop.fs.BlockLocation arg0, org.apache.hadoop.fs.BlockLocation
				 arg1)
			{
				long cmprv = arg0.getOffset() - arg1.getOffset();
				if (cmprv < 0)
				{
					return -1;
				}
				if (cmprv > 0)
				{
					return 1;
				}
				cmprv = arg0.getLength() - arg1.getLength();
				if (cmprv < 0)
				{
					return -1;
				}
				if (cmprv > 0)
				{
					return 1;
				}
				return 0;
			}
		}

		/// <seealso cref="NUnit.Framework.TestCase.tearDown()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			fs.delete(path, true);
			fs.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureNegativeParameters()
		{
			org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(path);
			try
			{
				org.apache.hadoop.fs.BlockLocation[] locations = fs.getFileBlockLocations(status, 
					-1, 100);
				fail("Expecting exception being throw");
			}
			catch (System.ArgumentException)
			{
			}
			try
			{
				org.apache.hadoop.fs.BlockLocation[] locations = fs.getFileBlockLocations(status, 
					100, -1);
				fail("Expecting exception being throw");
			}
			catch (System.ArgumentException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetFileBlockLocations1()
		{
			org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(path);
			oneTest(0, (int)status.getLen(), status);
			oneTest(0, (int)status.getLen() * 2, status);
			oneTest((int)status.getLen() * 2, (int)status.getLen() * 4, status);
			oneTest((int)status.getLen() / 2, (int)status.getLen() * 3, status);
			oneTest((int)status.getLen(), (int)status.getLen() * 2, status);
			for (int i = 0; i < 10; ++i)
			{
				oneTest((int)status.getLen() * i / 10, (int)status.getLen() * (i + 1) / 10, status
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetFileBlockLocations2()
		{
			org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(path);
			for (int i = 0; i < 1000; ++i)
			{
				int offBegin = random.nextInt((int)(2 * status.getLen()));
				int offEnd = random.nextInt((int)(2 * status.getLen()));
				oneTest(offBegin, offEnd, status);
			}
		}
	}
}
