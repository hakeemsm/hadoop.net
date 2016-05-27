using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public class TestCryptoStreamsForLocalFS : CryptoStreamsTestBase
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data") + "/work-dir/localfs";

		private readonly FilePath @base = new FilePath(TestRootDir);

		private readonly Path file = new Path(TestRootDir, "test-file");

		private static LocalFileSystem fileSys;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			Configuration conf = new Configuration();
			conf = new Configuration(false);
			conf.Set("fs.file.impl", typeof(LocalFileSystem).FullName);
			fileSys = FileSystem.GetLocal(conf);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityCryptoCodecClassesKeyPrefix 
				+ CipherSuite.AesCtrNopadding.GetConfigSuffix(), typeof(OpensslAesCtrCryptoCodec
				).FullName + "," + typeof(JceAesCtrCryptoCodec).FullName);
			codec = CryptoCodec.GetInstance(conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			fileSys.Delete(new Path(TestRootDir), true);
			base.SetUp();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void CleanUp()
		{
			FileUtil.SetWritable(@base, true);
			FileUtil.FullyDelete(@base);
			NUnit.Framework.Assert.IsTrue(!@base.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override OutputStream GetOutputStream(int bufferSize, byte[] key
			, byte[] iv)
		{
			return new CryptoOutputStream(fileSys.Create(file), codec, bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override InputStream GetInputStream(int bufferSize, byte[] key
			, byte[] iv)
		{
			return new CryptoInputStream(fileSys.Open(file), codec, bufferSize, key, iv);
		}

		/// <exception cref="System.Exception"/>
		public override void TestByteBufferRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestSyncable()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void TestCombinedOp()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void TestHasEnhancedByteBufferAccess()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void TestSeekToNewSource()
		{
		}
	}
}
