using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	public class TestTracingShortCircuitLocalRead
	{
		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem dfs;

		private static SpanReceiverHost spanReceiverHost;

		private static TemporarySocketDirectory sockDir;

		internal static readonly Path TestPath = new Path("testShortCircuitTraceHooks");

		internal const int TestLength = 1234;

		[BeforeClass]
		public static void Init()
		{
			sockDir = new TemporarySocketDirectory();
			DomainSocket.DisableBindPathValidation();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Shutdown()
		{
			sockDir.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitTraceHooks()
		{
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded() && !Path.Windows);
			conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsClientHtracePrefix + SpanReceiverHost.SpanReceiversConfSuffix
				, typeof(TestTracing.SetSpanReceiver).FullName);
			conf.SetLong("dfs.blocksize", 100 * 1024);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, "testShortCircuitTraceHooks._PORT"
				);
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "CRC32C");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			dfs = cluster.GetFileSystem();
			try
			{
				DFSTestUtil.CreateFile(dfs, TestPath, TestLength, (short)1, 5678L);
				TraceScope ts = Trace.StartSpan("testShortCircuitTraceHooks", Sampler.Always);
				FSDataInputStream stream = dfs.Open(TestPath);
				byte[] buf = new byte[TestLength];
				IOUtils.ReadFully(stream, buf, 0, TestLength);
				stream.Close();
				ts.Close();
				string[] expectedSpanNames = new string[] { "OpRequestShortCircuitAccessProto", "ShortCircuitShmRequestProto"
					 };
				TestTracing.AssertSpanNamesFound(expectedSpanNames);
			}
			finally
			{
				dfs.Close();
				cluster.Shutdown();
			}
		}
	}
}
