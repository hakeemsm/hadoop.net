using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestSecureIOUtils
	{
		private static string realOwner;

		private static string realGroup;

		private static java.io.File testFilePathIs;

		private static java.io.File testFilePathRaf;

		private static java.io.File testFilePathFadis;

		private static org.apache.hadoop.fs.FileSystem fs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void makeTestFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			fs = org.apache.hadoop.fs.FileSystem.getLocal(conf).getRaw();
			testFilePathIs = new java.io.File((new org.apache.hadoop.fs.Path("target", Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestSecureIOUtils)).getSimpleName() + "1")).toUri()
				.getRawPath());
			testFilePathRaf = new java.io.File((new org.apache.hadoop.fs.Path("target", Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestSecureIOUtils)).getSimpleName() + "2")).toUri()
				.getRawPath());
			testFilePathFadis = new java.io.File((new org.apache.hadoop.fs.Path("target", Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestSecureIOUtils)).getSimpleName() + "3")).toUri()
				.getRawPath());
			foreach (java.io.File f in new java.io.File[] { testFilePathIs, testFilePathRaf, 
				testFilePathFadis })
			{
				java.io.FileOutputStream fos = new java.io.FileOutputStream(f);
				fos.write(Sharpen.Runtime.getBytesForString("hello", "UTF-8"));
				fos.close();
			}
			org.apache.hadoop.fs.FileStatus stat = fs.getFileStatus(new org.apache.hadoop.fs.Path
				(testFilePathIs.ToString()));
			// RealOwner and RealGroup would be same for all three files.
			realOwner = stat.getOwner();
			realGroup = stat.getGroup();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReadUnrestricted()
		{
			org.apache.hadoop.io.SecureIOUtils.openForRead(testFilePathIs, null, null).close(
				);
			org.apache.hadoop.io.SecureIOUtils.openFSDataInputStream(testFilePathFadis, null, 
				null).close();
			org.apache.hadoop.io.SecureIOUtils.openForRandomRead(testFilePathRaf, "r", null, 
				null).close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReadCorrectlyRestrictedWithSecurity()
		{
			org.apache.hadoop.io.SecureIOUtils.openForRead(testFilePathIs, realOwner, realGroup
				).close();
			org.apache.hadoop.io.SecureIOUtils.openFSDataInputStream(testFilePathFadis, realOwner
				, realGroup).close();
			org.apache.hadoop.io.SecureIOUtils.openForRandomRead(testFilePathRaf, "r", realOwner
				, realGroup).close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReadIncorrectlyRestrictedWithSecurity()
		{
			// this will only run if libs are available
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.nativeio.NativeIO.isAvailable
				());
			System.Console.Out.WriteLine("Running test with native libs...");
			string invalidUser = "InvalidUser";
			// We need to make sure that forceSecure.. call works only if
			// the file belongs to expectedOwner.
			// InputStream
			try
			{
				org.apache.hadoop.io.SecureIOUtils.forceSecureOpenForRead(testFilePathIs, invalidUser
					, realGroup).close();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			// FSDataInputStream
			try
			{
				org.apache.hadoop.io.SecureIOUtils.forceSecureOpenFSDataInputStream(testFilePathFadis
					, invalidUser, realGroup).close();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			// RandomAccessFile
			try
			{
				org.apache.hadoop.io.SecureIOUtils.forceSecureOpenForRandomRead(testFilePathRaf, 
					"r", invalidUser, realGroup).close();
				NUnit.Framework.Assert.Fail("Didn't throw expection for wrong user ownership!");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateForWrite()
		{
			try
			{
				org.apache.hadoop.io.SecureIOUtils.createForWrite(testFilePathIs, 0x1ff);
				NUnit.Framework.Assert.Fail("Was able to create file at " + testFilePathIs);
			}
			catch (org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void removeTestFile()
		{
			// cleaning files
			foreach (java.io.File f in new java.io.File[] { testFilePathIs, testFilePathRaf, 
				testFilePathFadis })
			{
				f.delete();
			}
		}
	}
}
