using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Nativeio
{
	public class TestSharedFileDescriptorFactory
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestSharedFileDescriptorFactory
			));

		private static readonly FilePath TestBase = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Assume.AssumeTrue(null == SharedFileDescriptorFactory.GetLoadingFailureReason());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadAndWrite()
		{
			FilePath path = new FilePath(TestBase, "testReadAndWrite");
			path.Mkdirs();
			SharedFileDescriptorFactory factory = SharedFileDescriptorFactory.Create("woot_", 
				new string[] { path.GetAbsolutePath() });
			FileInputStream inStream = factory.CreateDescriptor("testReadAndWrite", 4096);
			FileOutputStream outStream = new FileOutputStream(inStream.GetFD());
			outStream.Write(101);
			inStream.GetChannel().Position(0);
			Assert.Equal(101, inStream.Read());
			inStream.Close();
			outStream.Close();
			FileUtil.FullyDelete(path);
		}

		/// <exception cref="System.Exception"/>
		private static void CreateTempFile(string path)
		{
			FileOutputStream fos = new FileOutputStream(path);
			fos.Write(101);
			fos.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCleanupRemainders()
		{
			Assume.AssumeTrue(NativeIO.IsAvailable());
			Assume.AssumeTrue(SystemUtils.IsOsUnix);
			FilePath path = new FilePath(TestBase, "testCleanupRemainders");
			path.Mkdirs();
			string remainder1 = path.GetAbsolutePath() + Path.Separator + "woot2_remainder1";
			string remainder2 = path.GetAbsolutePath() + Path.Separator + "woot2_remainder2";
			CreateTempFile(remainder1);
			CreateTempFile(remainder2);
			SharedFileDescriptorFactory.Create("woot2_", new string[] { path.GetAbsolutePath(
				) });
			// creating the SharedFileDescriptorFactory should have removed 
			// the remainders
			NUnit.Framework.Assert.IsFalse(new FilePath(remainder1).Exists());
			NUnit.Framework.Assert.IsFalse(new FilePath(remainder2).Exists());
			FileUtil.FullyDelete(path);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDirectoryFallbacks()
		{
			FilePath nonExistentPath = new FilePath(TestBase, "nonexistent");
			FilePath permissionDeniedPath = new FilePath("/");
			FilePath goodPath = new FilePath(TestBase, "testDirectoryFallbacks");
			goodPath.Mkdirs();
			try
			{
				SharedFileDescriptorFactory.Create("shm_", new string[] { nonExistentPath.GetAbsolutePath
					(), permissionDeniedPath.GetAbsolutePath() });
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			SharedFileDescriptorFactory factory = SharedFileDescriptorFactory.Create("shm_", 
				new string[] { nonExistentPath.GetAbsolutePath(), permissionDeniedPath.GetAbsolutePath
				(), goodPath.GetAbsolutePath() });
			Assert.Equal(goodPath.GetAbsolutePath(), factory.GetPath());
			FileUtil.FullyDelete(goodPath);
		}
	}
}
