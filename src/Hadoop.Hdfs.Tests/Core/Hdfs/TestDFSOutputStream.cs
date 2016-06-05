using System.IO;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSOutputStream
	{
		internal static MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			Configuration conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).Build();
		}

		/// <summary>
		/// The close() method of DFSOutputStream should never throw the same exception
		/// twice.
		/// </summary>
		/// <remarks>
		/// The close() method of DFSOutputStream should never throw the same exception
		/// twice. See HDFS-5335 for details.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCloseTwice()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			FSDataOutputStream os = fs.Create(new Path("/test"));
			DFSOutputStream dos = (DFSOutputStream)Whitebox.GetInternalState(os, "wrappedStream"
				);
			AtomicReference<IOException> ex = (AtomicReference<IOException>)Whitebox.GetInternalState
				(dos, "lastException");
			NUnit.Framework.Assert.AreEqual(null, ex.Get());
			dos.Close();
			IOException dummy = new IOException("dummy");
			ex.Set(dummy);
			try
			{
				dos.Close();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.AreEqual(e, dummy);
			}
			NUnit.Framework.Assert.AreEqual(null, ex.Get());
			dos.Close();
		}

		/// <summary>
		/// The computePacketChunkSize() method of DFSOutputStream should set the actual
		/// packet size &lt; 64kB.
		/// </summary>
		/// <remarks>
		/// The computePacketChunkSize() method of DFSOutputStream should set the actual
		/// packet size &lt; 64kB. See HDFS-7308 for details.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestComputePacketChunkSize()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			FSDataOutputStream os = fs.Create(new Path("/test"));
			DFSOutputStream dos = (DFSOutputStream)Whitebox.GetInternalState(os, "wrappedStream"
				);
			int packetSize = 64 * 1024;
			int bytesPerChecksum = 512;
			MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(dos.GetType(), "computePacketChunkSize"
				, typeof(int), typeof(int));
			method.Invoke(dos, packetSize, bytesPerChecksum);
			FieldInfo field = Sharpen.Runtime.GetDeclaredField(dos.GetType(), "packetSize");
			NUnit.Framework.Assert.IsTrue((int)field.GetValue(dos) + 33 < packetSize);
			// If PKT_MAX_HEADER_LEN is 257, actual packet size come to over 64KB
			// without a fix on HDFS-7308.
			NUnit.Framework.Assert.IsTrue((int)field.GetValue(dos) + 257 < packetSize);
		}

		[AfterClass]
		public static void TearDown()
		{
			cluster.Shutdown();
		}
	}
}
