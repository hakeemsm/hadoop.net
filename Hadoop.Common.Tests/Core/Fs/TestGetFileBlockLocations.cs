using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Testing the correctness of FileSystem.getFileBlockLocations.</summary>
	public class TestGetFileBlockLocations : TestCase
	{
		private static string TestRootDir = Runtime.GetProperty("test.build.data", "/tmp/testGetFileBlockLocations"
			);

		private const int FileLength = 4 * 1024 * 1024;

		private Configuration conf;

		private Path path;

		private FileSystem fs;

		private Random random;

		// 4MB
		/// <seealso cref="NUnit.Framework.TestCase.SetUp()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			Path rootPath = new Path(TestRootDir);
			path = new Path(rootPath, "TestGetFileBlockLocations");
			fs = rootPath.GetFileSystem(conf);
			FSDataOutputStream fsdos = fs.Create(path, true);
			byte[] buffer = new byte[1024];
			while (fsdos.GetPos() < FileLength)
			{
				fsdos.Write(buffer);
			}
			fsdos.Close();
			random = new Random(Runtime.NanoTime());
		}

		/// <exception cref="System.IO.IOException"/>
		private void OneTest(int offBegin, int offEnd, FileStatus status)
		{
			if (offBegin > offEnd)
			{
				int tmp = offBegin;
				offBegin = offEnd;
				offEnd = tmp;
			}
			BlockLocation[] locations = fs.GetFileBlockLocations(status, offBegin, offEnd - offBegin
				);
			if (offBegin < status.GetLen())
			{
				Arrays.Sort(locations, new _IComparer_69());
				offBegin = (int)Math.Min(offBegin, status.GetLen() - 1);
				offEnd = (int)Math.Min(offEnd, status.GetLen());
				BlockLocation first = locations[0];
				BlockLocation last = locations[locations.Length - 1];
				NUnit.Framework.Assert.IsTrue(first.GetOffset() <= offBegin);
				NUnit.Framework.Assert.IsTrue(offEnd <= last.GetOffset() + last.GetLength());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(locations.Length == 0);
			}
		}

		private sealed class _IComparer_69 : IComparer<BlockLocation>
		{
			public _IComparer_69()
			{
			}

			public int Compare(BlockLocation arg0, BlockLocation arg1)
			{
				long cmprv = arg0.GetOffset() - arg1.GetOffset();
				if (cmprv < 0)
				{
					return -1;
				}
				if (cmprv > 0)
				{
					return 1;
				}
				cmprv = arg0.GetLength() - arg1.GetLength();
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

		/// <seealso cref="NUnit.Framework.TestCase.TearDown()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			fs.Delete(path, true);
			fs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureNegativeParameters()
		{
			FileStatus status = fs.GetFileStatus(path);
			try
			{
				BlockLocation[] locations = fs.GetFileBlockLocations(status, -1, 100);
				Fail("Expecting exception being throw");
			}
			catch (ArgumentException)
			{
			}
			try
			{
				BlockLocation[] locations = fs.GetFileBlockLocations(status, 100, -1);
				Fail("Expecting exception being throw");
			}
			catch (ArgumentException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetFileBlockLocations1()
		{
			FileStatus status = fs.GetFileStatus(path);
			OneTest(0, (int)status.GetLen(), status);
			OneTest(0, (int)status.GetLen() * 2, status);
			OneTest((int)status.GetLen() * 2, (int)status.GetLen() * 4, status);
			OneTest((int)status.GetLen() / 2, (int)status.GetLen() * 3, status);
			OneTest((int)status.GetLen(), (int)status.GetLen() * 2, status);
			for (int i = 0; i < 10; ++i)
			{
				OneTest((int)status.GetLen() * i / 10, (int)status.GetLen() * (i + 1) / 10, status
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetFileBlockLocations2()
		{
			FileStatus status = fs.GetFileStatus(path);
			for (int i = 0; i < 1000; ++i)
			{
				int offBegin = random.Next((int)(2 * status.GetLen()));
				int offEnd = random.Next((int)(2 * status.GetLen()));
				OneTest(offBegin, offEnd, status);
			}
		}
	}
}
