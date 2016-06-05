using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This test makes sure that "DU" does not get to run on each call to getUsed
	/// 	</summary>
	public class TestDU : TestCase
	{
		private static readonly FilePath DuDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "dutmp");

		protected override void SetUp()
		{
			FileUtil.FullyDelete(DuDir);
			Assert.True(DuDir.Mkdirs());
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			FileUtil.FullyDelete(DuDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(FilePath newFile, int size)
		{
			// write random data so that filesystems with compression enabled (e.g., ZFS)
			// can't compress the file
			Random random = new Random();
			byte[] data = new byte[size];
			random.NextBytes(data);
			newFile.CreateNewFile();
			RandomAccessFile file = new RandomAccessFile(newFile, "rws");
			file.Write(data);
			file.GetFD().Sync();
			file.Close();
		}

		/// <summary>Verify that du returns expected used space for a file.</summary>
		/// <remarks>
		/// Verify that du returns expected used space for a file.
		/// We assume here that if a file system crates a file of size
		/// that is a multiple of the block size in this file system,
		/// then the used size for the file will be exactly that size.
		/// This is true for most file systems.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDU()
		{
			int writtenSize = 32 * 1024;
			// writing 32K
			// Allow for extra 4K on-disk slack for local file systems
			// that may store additional file metadata (eg ext attrs).
			int slack = 4 * 1024;
			FilePath file = new FilePath(DuDir, "data");
			CreateFile(file, writtenSize);
			Sharpen.Thread.Sleep(5000);
			// let the metadata updater catch up
			DU du = new DU(file, 10000);
			du.Start();
			long duSize = du.GetUsed();
			du.Shutdown();
			Assert.True("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
			//test with 0 interval, will not launch thread 
			du = new DU(file, 0);
			du.Start();
			duSize = du.GetUsed();
			du.Shutdown();
			Assert.True("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
			//test without launching thread 
			du = new DU(file, 10000);
			duSize = du.GetUsed();
			Assert.True("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDUGetUsedWillNotReturnNegative()
		{
			FilePath file = new FilePath(DuDir, "data");
			Assert.True(file.CreateNewFile());
			Configuration conf = new Configuration();
			conf.SetLong(CommonConfigurationKeys.FsDuIntervalKey, 10000L);
			DU du = new DU(file, conf);
			du.DecDfsUsed(long.MaxValue);
			long duSize = du.GetUsed();
			Assert.True(duSize.ToString(), duSize >= 0L);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDUSetInitialValue()
		{
			FilePath file = new FilePath(DuDir, "dataX");
			CreateFile(file, 8192);
			DU du = new DU(file, 3000, 1024);
			du.Start();
			Assert.True("Initial usage setting not honored", du.GetUsed() ==
				 1024);
			// wait until the first du runs.
			try
			{
				Sharpen.Thread.Sleep(5000);
			}
			catch (Exception)
			{
			}
			Assert.True("Usage didn't get updated", du.GetUsed() == 8192);
		}
	}
}
