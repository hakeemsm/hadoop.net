using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestMD5FileUtils
	{
		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestMD5FileUtils
			));

		private static readonly FilePath TestFile = new FilePath(TestDir, "testMd5File.dat"
			);

		private const int TestDataLen = 128 * 1024;

		private static readonly byte[] TestData = DFSTestUtil.GenerateSequentialBytes(0, 
			TestDataLen);

		private static readonly MD5Hash TestMd5 = MD5Hash.Digest(TestData);

		// 128KB test data
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(TestDir);
			NUnit.Framework.Assert.IsTrue(TestDir.Mkdirs());
			// Write a file out
			FileOutputStream fos = new FileOutputStream(TestFile);
			fos.Write(TestData);
			fos.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestComputeMd5ForFile()
		{
			MD5Hash computedDigest = MD5FileUtils.ComputeMd5ForFile(TestFile);
			NUnit.Framework.Assert.AreEqual(TestMd5, computedDigest);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyMD5FileGood()
		{
			MD5FileUtils.SaveMD5File(TestFile, TestMd5);
			MD5FileUtils.VerifySavedMD5(TestFile, TestMd5);
		}

		/// <summary>Test when .md5 file does not exist at all</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestVerifyMD5FileMissing()
		{
			MD5FileUtils.VerifySavedMD5(TestFile, TestMd5);
		}

		/// <summary>Test when .md5 file exists but incorrect checksum</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyMD5FileBadDigest()
		{
			MD5FileUtils.SaveMD5File(TestFile, MD5Hash.Digest(new byte[0]));
			try
			{
				MD5FileUtils.VerifySavedMD5(TestFile, TestMd5);
				NUnit.Framework.Assert.Fail("Did not throw");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <summary>Test when .md5 file exists but has a bad format</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyMD5FileBadFormat()
		{
			FileWriter writer = new FileWriter(MD5FileUtils.GetDigestFileForFile(TestFile));
			try
			{
				writer.Write("this is not an md5 file");
			}
			finally
			{
				writer.Close();
			}
			try
			{
				MD5FileUtils.VerifySavedMD5(TestFile, TestMd5);
				NUnit.Framework.Assert.Fail("Did not throw");
			}
			catch (IOException)
			{
			}
		}
		// expected
	}
}
