using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Crypto;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Crypto
{
	public class TestHdfsCryptoStreams : CryptoStreamsTestBase
	{
		private static MiniDFSCluster dfsCluster;

		private static FileSystem fs;

		private static int pathCount = 0;

		private static Path path;

		private static Path file;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			Configuration conf = new HdfsConfiguration();
			dfsCluster = new MiniDFSCluster.Builder(conf).Build();
			dfsCluster.WaitClusterUp();
			fs = dfsCluster.GetFileSystem();
			codec = CryptoCodec.GetInstance(conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			++pathCount;
			path = new Path("/p" + pathCount);
			file = new Path(path, "file");
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1c0));
			base.SetUp();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void CleanUp()
		{
			fs.Delete(path, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override OutputStream GetOutputStream(int bufferSize, byte[] key, byte[]
			 iv)
		{
			return new CryptoFSDataOutputStream(fs.Create(file), codec, bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override InputStream GetInputStream(int bufferSize, byte[] key, byte[] 
			iv)
		{
			return new CryptoFSDataInputStream(fs.Open(file), codec, bufferSize, key, iv);
		}
	}
}
