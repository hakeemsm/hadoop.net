using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This test makes sure that "DU" does not get to run on each call to getUsed
	/// 	</summary>
	public class TestDU : NUnit.Framework.TestCase
	{
		private static readonly java.io.File DU_DIR = new java.io.File(Sharpen.Runtime.getProperty
			("test.build.data", "/tmp"), "dutmp");

		protected override void setUp()
		{
			org.apache.hadoop.fs.FileUtil.fullyDelete(DU_DIR);
			NUnit.Framework.Assert.IsTrue(DU_DIR.mkdirs());
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			org.apache.hadoop.fs.FileUtil.fullyDelete(DU_DIR);
		}

		/// <exception cref="System.IO.IOException"/>
		private void createFile(java.io.File newFile, int size)
		{
			// write random data so that filesystems with compression enabled (e.g., ZFS)
			// can't compress the file
			java.util.Random random = new java.util.Random();
			byte[] data = new byte[size];
			random.nextBytes(data);
			newFile.createNewFile();
			java.io.RandomAccessFile file = new java.io.RandomAccessFile(newFile, "rws");
			file.write(data);
			file.getFD().sync();
			file.close();
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
		public virtual void testDU()
		{
			int writtenSize = 32 * 1024;
			// writing 32K
			// Allow for extra 4K on-disk slack for local file systems
			// that may store additional file metadata (eg ext attrs).
			int slack = 4 * 1024;
			java.io.File file = new java.io.File(DU_DIR, "data");
			createFile(file, writtenSize);
			java.lang.Thread.sleep(5000);
			// let the metadata updater catch up
			org.apache.hadoop.fs.DU du = new org.apache.hadoop.fs.DU(file, 10000);
			du.start();
			long duSize = du.getUsed();
			du.shutdown();
			NUnit.Framework.Assert.IsTrue("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
			//test with 0 interval, will not launch thread 
			du = new org.apache.hadoop.fs.DU(file, 0);
			du.start();
			duSize = du.getUsed();
			du.shutdown();
			NUnit.Framework.Assert.IsTrue("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
			//test without launching thread 
			du = new org.apache.hadoop.fs.DU(file, 10000);
			duSize = du.getUsed();
			NUnit.Framework.Assert.IsTrue("Invalid on-disk size", duSize >= writtenSize && writtenSize
				 <= (duSize + slack));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDUGetUsedWillNotReturnNegative()
		{
			java.io.File file = new java.io.File(DU_DIR, "data");
			NUnit.Framework.Assert.IsTrue(file.createNewFile());
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setLong(org.apache.hadoop.fs.CommonConfigurationKeys.FS_DU_INTERVAL_KEY, 10000L
				);
			org.apache.hadoop.fs.DU du = new org.apache.hadoop.fs.DU(file, conf);
			du.decDfsUsed(long.MaxValue);
			long duSize = du.getUsed();
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.getStringValueOf(duSize), duSize >=
				 0L);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDUSetInitialValue()
		{
			java.io.File file = new java.io.File(DU_DIR, "dataX");
			createFile(file, 8192);
			org.apache.hadoop.fs.DU du = new org.apache.hadoop.fs.DU(file, 3000, 1024);
			du.start();
			NUnit.Framework.Assert.IsTrue("Initial usage setting not honored", du.getUsed() ==
				 1024);
			// wait until the first du runs.
			try
			{
				java.lang.Thread.sleep(5000);
			}
			catch (System.Exception)
			{
			}
			NUnit.Framework.Assert.IsTrue("Usage didn't get updated", du.getUsed() == 8192);
		}
	}
}
