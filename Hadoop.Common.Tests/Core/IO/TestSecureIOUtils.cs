using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestSecureIOUtils
	{
		private static string realOwner;

		private static string realGroup;

		private static FilePath testFilePathIs;

		private static FilePath testFilePathRaf;

		private static FilePath testFilePathFadis;

		private static FileSystem fs;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void MakeTestFile()
		{
			Configuration conf = new Configuration();
			fs = FileSystem.GetLocal(conf).GetRaw();
			testFilePathIs = new FilePath((new Path("target", typeof(TestSecureIOUtils).Name 
				+ "1")).ToUri().GetRawPath());
			testFilePathRaf = new FilePath((new Path("target", typeof(TestSecureIOUtils).Name
				 + "2")).ToUri().GetRawPath());
			testFilePathFadis = new FilePath((new Path("target", typeof(TestSecureIOUtils).Name
				 + "3")).ToUri().GetRawPath());
			foreach (FilePath f in new FilePath[] { testFilePathIs, testFilePathRaf, testFilePathFadis
				 })
			{
				FileOutputStream fos = new FileOutputStream(f);
				fos.Write(Sharpen.Runtime.GetBytesForString("hello", "UTF-8"));
				fos.Close();
			}
			FileStatus stat = fs.GetFileStatus(new Path(testFilePathIs.ToString()));
			// RealOwner and RealGroup would be same for all three files.
			realOwner = stat.GetOwner();
			realGroup = stat.GetGroup();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReadUnrestricted()
		{
			SecureIOUtils.OpenForRead(testFilePathIs, null, null).Close();
			SecureIOUtils.OpenFSDataInputStream(testFilePathFadis, null, null).Close();
			SecureIOUtils.OpenForRandomRead(testFilePathRaf, "r", null, null).Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReadCorrectlyRestrictedWithSecurity()
		{
			SecureIOUtils.OpenForRead(testFilePathIs, realOwner, realGroup).Close();
			SecureIOUtils.OpenFSDataInputStream(testFilePathFadis, realOwner, realGroup).Close
				();
			SecureIOUtils.OpenForRandomRead(testFilePathRaf, "r", realOwner, realGroup).Close
				();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReadIncorrectlyRestrictedWithSecurity()
		{
			// this will only run if libs are available
			Assume.AssumeTrue(NativeIO.IsAvailable());
			System.Console.Out.WriteLine("Running test with native libs...");
			string invalidUser = "InvalidUser";
			// We need to make sure that forceSecure.. call works only if
			// the file belongs to expectedOwner.
			// InputStream
			try
			{
				SecureIOUtils.ForceSecureOpenForRead(testFilePathIs, invalidUser, realGroup).Close
					();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (IOException)
			{
			}
			// expected
			// FSDataInputStream
			try
			{
				SecureIOUtils.ForceSecureOpenFSDataInputStream(testFilePathFadis, invalidUser, realGroup
					).Close();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (IOException)
			{
			}
			// expected
			// RandomAccessFile
			try
			{
				SecureIOUtils.ForceSecureOpenForRandomRead(testFilePathRaf, "r", invalidUser, realGroup
					).Close();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (IOException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateForWrite()
		{
			try
			{
				SecureIOUtils.CreateForWrite(testFilePathIs, 0x1ff);
				NUnit.Framework.Assert.Fail("Was able to create file at " + testFilePathIs);
			}
			catch (SecureIOUtils.AlreadyExistsException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void RemoveTestFile()
		{
			// cleaning files
			foreach (FilePath f in new FilePath[] { testFilePathIs, testFilePathRaf, testFilePathFadis
				 })
			{
				f.Delete();
			}
		}
	}
}
