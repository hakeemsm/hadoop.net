using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	public class TestTracing
	{
		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem dfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTracing()
		{
			// write and read without tracing started
			string fileName = "testTracingDisabled.dat";
			WriteTestFile(fileName);
			NUnit.Framework.Assert.IsTrue(TestTracing.SetSpanReceiver.SetHolder.Size() == 0);
			ReadTestFile(fileName);
			NUnit.Framework.Assert.IsTrue(TestTracing.SetSpanReceiver.SetHolder.Size() == 0);
			WriteWithTracing();
			ReadWithTracing();
		}

		/// <exception cref="System.Exception"/>
		public virtual void WriteWithTracing()
		{
			long startTime = Runtime.CurrentTimeMillis();
			TraceScope ts = Trace.StartSpan("testWriteTraceHooks", Sampler.Always);
			WriteTestFile("testWriteTraceHooks.dat");
			long endTime = Runtime.CurrentTimeMillis();
			ts.Close();
			string[] expectedSpanNames = new string[] { "testWriteTraceHooks", "org.apache.hadoop.hdfs.protocol.ClientProtocol.create"
				, "ClientNamenodeProtocol#create", "org.apache.hadoop.hdfs.protocol.ClientProtocol.fsync"
				, "ClientNamenodeProtocol#fsync", "org.apache.hadoop.hdfs.protocol.ClientProtocol.complete"
				, "ClientNamenodeProtocol#complete", "newStreamForCreate", "DFSOutputStream#writeChunk"
				, "DFSOutputStream#close", "dataStreamer", "OpWriteBlockProto", "org.apache.hadoop.hdfs.protocol.ClientProtocol.addBlock"
				, "ClientNamenodeProtocol#addBlock" };
			AssertSpanNamesFound(expectedSpanNames);
			// The trace should last about the same amount of time as the test
			IDictionary<string, IList<Span>> map = TestTracing.SetSpanReceiver.SetHolder.GetMap
				();
			Span s = map["testWriteTraceHooks"][0];
			NUnit.Framework.Assert.IsNotNull(s);
			long spanStart = s.GetStartTimeMillis();
			long spanEnd = s.GetStopTimeMillis();
			// Spans homed in the top trace shoud have same trace id.
			// Spans having multiple parents (e.g. "dataStreamer" added by HDFS-7054)
			// and children of them are exception.
			string[] spansInTopTrace = new string[] { "testWriteTraceHooks", "org.apache.hadoop.hdfs.protocol.ClientProtocol.create"
				, "ClientNamenodeProtocol#create", "org.apache.hadoop.hdfs.protocol.ClientProtocol.fsync"
				, "ClientNamenodeProtocol#fsync", "org.apache.hadoop.hdfs.protocol.ClientProtocol.complete"
				, "ClientNamenodeProtocol#complete", "newStreamForCreate", "DFSOutputStream#writeChunk"
				, "DFSOutputStream#close" };
			foreach (string desc in spansInTopTrace)
			{
				foreach (Span span in map[desc])
				{
					NUnit.Framework.Assert.AreEqual(ts.GetSpan().GetTraceId(), span.GetTraceId());
				}
			}
			TestTracing.SetSpanReceiver.SetHolder.spans.Clear();
		}

		/// <exception cref="System.Exception"/>
		public virtual void ReadWithTracing()
		{
			string fileName = "testReadTraceHooks.dat";
			WriteTestFile(fileName);
			long startTime = Runtime.CurrentTimeMillis();
			TraceScope ts = Trace.StartSpan("testReadTraceHooks", Sampler.Always);
			ReadTestFile(fileName);
			ts.Close();
			long endTime = Runtime.CurrentTimeMillis();
			string[] expectedSpanNames = new string[] { "testReadTraceHooks", "org.apache.hadoop.hdfs.protocol.ClientProtocol.getBlockLocations"
				, "ClientNamenodeProtocol#getBlockLocations", "OpReadBlockProto" };
			AssertSpanNamesFound(expectedSpanNames);
			// The trace should last about the same amount of time as the test
			IDictionary<string, IList<Span>> map = TestTracing.SetSpanReceiver.SetHolder.GetMap
				();
			Span s = map["testReadTraceHooks"][0];
			NUnit.Framework.Assert.IsNotNull(s);
			long spanStart = s.GetStartTimeMillis();
			long spanEnd = s.GetStopTimeMillis();
			NUnit.Framework.Assert.IsTrue(spanStart - startTime < 100);
			NUnit.Framework.Assert.IsTrue(spanEnd - endTime < 100);
			// There should only be one trace id as it should all be homed in the
			// top trace.
			foreach (Span span in TestTracing.SetSpanReceiver.SetHolder.spans.Values)
			{
				NUnit.Framework.Assert.AreEqual(ts.GetSpan().GetTraceId(), span.GetTraceId());
			}
			TestTracing.SetSpanReceiver.SetHolder.spans.Clear();
		}

		/// <exception cref="System.Exception"/>
		private void WriteTestFile(string testFileName)
		{
			Path filePath = new Path(testFileName);
			FSDataOutputStream stream = dfs.Create(filePath);
			for (int i = 0; i < 10; i++)
			{
				byte[] data = Sharpen.Runtime.GetBytesForString(RandomStringUtils.RandomAlphabetic
					(102400));
				stream.Write(data);
			}
			stream.Hsync();
			stream.Close();
		}

		/// <exception cref="System.Exception"/>
		private void ReadTestFile(string testFileName)
		{
			Path filePath = new Path(testFileName);
			FSDataInputStream istream = dfs.Open(filePath, 10240);
			ByteBuffer buf = ByteBuffer.Allocate(10240);
			int count = 0;
			try
			{
				while (istream.Read(buf) > 0)
				{
					count += 1;
					buf.Clear();
					istream.Seek(istream.GetPos() + 5);
				}
			}
			catch (IOException)
			{
			}
			finally
			{
				// Ignore this it's probably a seek after eof.
				istream.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.SetLong("dfs.blocksize", 100 * 1024);
			conf.Set(DFSConfigKeys.DfsClientHtracePrefix + SpanReceiverHost.SpanReceiversConfSuffix
				, typeof(TestTracing.SetSpanReceiver).FullName);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
			TestTracing.SetSpanReceiver.SetHolder.spans.Clear();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDown()
		{
			cluster.Shutdown();
		}

		internal static void AssertSpanNamesFound(string[] expectedSpanNames)
		{
			try
			{
				GenericTestUtils.WaitFor(new _Supplier_217(expectedSpanNames), 100, 1000);
			}
			catch (TimeoutException e)
			{
				NUnit.Framework.Assert.Fail("timed out to get expected spans: " + e.Message);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("interrupted while waiting spans: " + e.Message);
			}
		}

		private sealed class _Supplier_217 : Supplier<bool>
		{
			public _Supplier_217(string[] expectedSpanNames)
			{
				this.expectedSpanNames = expectedSpanNames;
			}

			public bool Get()
			{
				IDictionary<string, IList<Span>> map = TestTracing.SetSpanReceiver.SetHolder.GetMap
					();
				foreach (string spanName in expectedSpanNames)
				{
					if (!map.Contains(spanName))
					{
						return false;
					}
				}
				return true;
			}

			private readonly string[] expectedSpanNames;
		}

		/// <summary>Span receiver that puts all spans into a single set.</summary>
		/// <remarks>
		/// Span receiver that puts all spans into a single set.
		/// This is useful for testing.
		/// <p/>
		/// We're not using HTrace's POJOReceiver here so as that doesn't
		/// push all the metrics to a static place, and would make testing
		/// SpanReceiverHost harder.
		/// </remarks>
		public class SetSpanReceiver : SpanReceiver
		{
			public SetSpanReceiver(HTraceConfiguration conf)
			{
			}

			public virtual void ReceiveSpan(Span span)
			{
				TestTracing.SetSpanReceiver.SetHolder.spans[span.GetSpanId()] = span;
			}

			public virtual void Close()
			{
			}

			public class SetHolder
			{
				public static ConcurrentHashMap<long, Span> spans = new ConcurrentHashMap<long, Span
					>();

				public static int Size()
				{
					return spans.Count;
				}

				public static IDictionary<string, IList<Span>> GetMap()
				{
					IDictionary<string, IList<Span>> map = new Dictionary<string, IList<Span>>();
					foreach (Span s in spans.Values)
					{
						IList<Span> l = map[s.GetDescription()];
						if (l == null)
						{
							l = new List<Span>();
							map[s.GetDescription()] = l;
						}
						l.AddItem(s);
					}
					return map;
				}
			}
		}
	}
}
