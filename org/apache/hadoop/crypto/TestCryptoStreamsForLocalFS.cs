using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class TestCryptoStreamsForLocalFS : org.apache.hadoop.crypto.CryptoStreamsTestBase
	{
		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data") + "/work-dir/localfs";

		private readonly java.io.File @base = new java.io.File(TEST_ROOT_DIR);

		private readonly org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR
			, "test-file");

		private static org.apache.hadoop.fs.LocalFileSystem fileSys;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void init()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf = new org.apache.hadoop.conf.Configuration(false);
			conf.set("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.LocalFileSystem
				)).getName());
			fileSys = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
				 + org.apache.hadoop.crypto.CipherSuite.AES_CTR_NOPADDING.getConfigSuffix(), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec)).getName() + "," + Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.crypto.JceAesCtrCryptoCodec)).getName());
			codec = org.apache.hadoop.crypto.CryptoCodec.getInstance(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void shutdown()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			fileSys.delete(new org.apache.hadoop.fs.Path(TEST_ROOT_DIR), true);
			base.setUp();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void cleanUp()
		{
			org.apache.hadoop.fs.FileUtil.setWritable(@base, true);
			org.apache.hadoop.fs.FileUtil.fullyDelete(@base);
			NUnit.Framework.Assert.IsTrue(!@base.exists());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.OutputStream getOutputStream(int bufferSize, 
			byte[] key, byte[] iv)
		{
			return new org.apache.hadoop.crypto.CryptoOutputStream(fileSys.create(file), codec
				, bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.InputStream getInputStream(int bufferSize, byte
			[] key, byte[] iv)
		{
			return new org.apache.hadoop.crypto.CryptoInputStream(fileSys.open(file), codec, 
				bufferSize, key, iv);
		}

		/// <exception cref="System.Exception"/>
		public override void testByteBufferRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testSyncable()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void testCombinedOp()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void testHasEnhancedByteBufferAccess()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void testSeekToNewSource()
		{
		}
	}
}
