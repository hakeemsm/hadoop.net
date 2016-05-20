using Sharpen;

namespace org.apache.hadoop.io.nativeio
{
	public class TestSharedFileDescriptorFactory
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.nativeio.TestSharedFileDescriptorFactory
			)));

		private static readonly java.io.File TEST_BASE = new java.io.File(Sharpen.Runtime
			.getProperty("test.build.data", "/tmp"));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			NUnit.Framework.Assume.assumeTrue(null == org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory
				.getLoadingFailureReason());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReadAndWrite()
		{
			java.io.File path = new java.io.File(TEST_BASE, "testReadAndWrite");
			path.mkdirs();
			org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory factory = org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory
				.create("woot_", new string[] { path.getAbsolutePath() });
			java.io.FileInputStream inStream = factory.createDescriptor("testReadAndWrite", 4096
				);
			java.io.FileOutputStream outStream = new java.io.FileOutputStream(inStream.getFD(
				));
			outStream.write(101);
			inStream.getChannel().position(0);
			NUnit.Framework.Assert.AreEqual(101, inStream.read());
			inStream.close();
			outStream.close();
			org.apache.hadoop.fs.FileUtil.fullyDelete(path);
		}

		/// <exception cref="System.Exception"/>
		private static void createTempFile(string path)
		{
			java.io.FileOutputStream fos = new java.io.FileOutputStream(path);
			fos.write(101);
			fos.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCleanupRemainders()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.nativeio.NativeIO.isAvailable
				());
			NUnit.Framework.Assume.assumeTrue(org.apache.commons.lang.SystemUtils.IS_OS_UNIX);
			java.io.File path = new java.io.File(TEST_BASE, "testCleanupRemainders");
			path.mkdirs();
			string remainder1 = path.getAbsolutePath() + org.apache.hadoop.fs.Path.SEPARATOR 
				+ "woot2_remainder1";
			string remainder2 = path.getAbsolutePath() + org.apache.hadoop.fs.Path.SEPARATOR 
				+ "woot2_remainder2";
			createTempFile(remainder1);
			createTempFile(remainder2);
			org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory.create("woot2_", new string
				[] { path.getAbsolutePath() });
			// creating the SharedFileDescriptorFactory should have removed 
			// the remainders
			NUnit.Framework.Assert.IsFalse(new java.io.File(remainder1).exists());
			NUnit.Framework.Assert.IsFalse(new java.io.File(remainder2).exists());
			org.apache.hadoop.fs.FileUtil.fullyDelete(path);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDirectoryFallbacks()
		{
			java.io.File nonExistentPath = new java.io.File(TEST_BASE, "nonexistent");
			java.io.File permissionDeniedPath = new java.io.File("/");
			java.io.File goodPath = new java.io.File(TEST_BASE, "testDirectoryFallbacks");
			goodPath.mkdirs();
			try
			{
				org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory.create("shm_", new string
					[] { nonExistentPath.getAbsolutePath(), permissionDeniedPath.getAbsolutePath() }
					);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.IO.IOException)
			{
			}
			org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory factory = org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory
				.create("shm_", new string[] { nonExistentPath.getAbsolutePath(), permissionDeniedPath
				.getAbsolutePath(), goodPath.getAbsolutePath() });
			NUnit.Framework.Assert.AreEqual(goodPath.getAbsolutePath(), factory.getPath());
			org.apache.hadoop.fs.FileUtil.fullyDelete(goodPath);
		}
	}
}
