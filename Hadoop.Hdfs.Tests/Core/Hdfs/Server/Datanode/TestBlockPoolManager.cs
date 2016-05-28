using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Mockito.Internal.Util.Reflection;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestBlockPoolManager
	{
		private readonly Log Log = LogFactory.GetLog(typeof(TestBlockPoolManager));

		private readonly DataNode mockDN = Org.Mockito.Mockito.Mock<DataNode>();

		private BlockPoolManager bpm;

		private readonly StringBuilder log = new StringBuilder();

		private int mockIdx = 1;

		[SetUp]
		public virtual void SetupBPM()
		{
			bpm = new _BlockPoolManager_51(this, mockDN);
		}

		private sealed class _BlockPoolManager_51 : BlockPoolManager
		{
			public _BlockPoolManager_51(TestBlockPoolManager _enclosing, DataNode baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override BPOfferService CreateBPOS(IList<IPEndPoint> nnAddrs)
			{
				int idx = this._enclosing.mockIdx++;
				this._enclosing.DoLog("create #" + idx);
				BPOfferService bpos = Org.Mockito.Mockito.Mock<BPOfferService>();
				Org.Mockito.Mockito.DoReturn("Mock BPOS #" + idx).When(bpos).ToString();
				// Log refreshes
				try
				{
					Org.Mockito.Mockito.DoAnswer(new _Answer_62(this, idx)).When(bpos).RefreshNNList(
						Org.Mockito.Mockito.Any<AList<IPEndPoint>>());
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
				// Log stops
				Org.Mockito.Mockito.DoAnswer(new _Answer_75(this, idx, bpos)).When(bpos).Stop();
				return bpos;
			}

			private sealed class _Answer_62 : Answer<Void>
			{
				public _Answer_62(_BlockPoolManager_51 _enclosing, int idx)
				{
					this._enclosing = _enclosing;
					this.idx = idx;
				}

				/// <exception cref="System.Exception"/>
				public Void Answer(InvocationOnMock invocation)
				{
					this._enclosing._enclosing.DoLog("refresh #" + idx);
					return null;
				}

				private readonly _BlockPoolManager_51 _enclosing;

				private readonly int idx;
			}

			private sealed class _Answer_75 : Answer<Void>
			{
				public _Answer_75(_BlockPoolManager_51 _enclosing, int idx, BPOfferService bpos)
				{
					this._enclosing = _enclosing;
					this.idx = idx;
					this.bpos = bpos;
				}

				/// <exception cref="System.Exception"/>
				public Void Answer(InvocationOnMock invocation)
				{
					this._enclosing._enclosing.DoLog("stop #" + idx);
					this._enclosing._enclosing.bpm.Remove(bpos);
					return null;
				}

				private readonly _BlockPoolManager_51 _enclosing;

				private readonly int idx;

				private readonly BPOfferService bpos;
			}

			private readonly TestBlockPoolManager _enclosing;
		}

		private void DoLog(string @string)
		{
			lock (log)
			{
				Log.Info(@string);
				log.Append(@string).Append("\n");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleSingleNS()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.FsDefaultNameKey, "hdfs://mock1:8020");
			bpm.RefreshNamenodes(conf);
			NUnit.Framework.Assert.AreEqual("create #1\n", log.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFederationRefresh()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2");
			AddNN(conf, "ns1", "mock1:8020");
			AddNN(conf, "ns2", "mock1:8020");
			bpm.RefreshNamenodes(conf);
			NUnit.Framework.Assert.AreEqual("create #1\n" + "create #2\n", log.ToString());
			log.Length = 0;
			// Remove the first NS
			conf.Set(DFSConfigKeys.DfsNameservices, "ns2");
			bpm.RefreshNamenodes(conf);
			NUnit.Framework.Assert.AreEqual("stop #1\n" + "refresh #2\n", log.ToString());
			log.Length = 0;
			// Add back an NS -- this creates a new BPOS since the old
			// one for ns2 should have been previously retired
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2");
			bpm.RefreshNamenodes(conf);
			NUnit.Framework.Assert.AreEqual("create #3\n" + "refresh #2\n", log.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInternalNameService()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2,ns3");
			AddNN(conf, "ns1", "mock1:8020");
			AddNN(conf, "ns2", "mock1:8020");
			AddNN(conf, "ns3", "mock1:8020");
			conf.Set(DFSConfigKeys.DfsInternalNameservicesKey, "ns1");
			bpm.RefreshNamenodes(conf);
			NUnit.Framework.Assert.AreEqual("create #1\n", log.ToString());
			IDictionary<string, BPOfferService> map = (IDictionary<string, BPOfferService>)Whitebox
				.GetInternalState(bpm, "bpByNameserviceId");
			NUnit.Framework.Assert.IsFalse(map.Contains("ns2"));
			NUnit.Framework.Assert.IsFalse(map.Contains("ns3"));
			NUnit.Framework.Assert.IsTrue(map.Contains("ns1"));
			log.Length = 0;
		}

		private static void AddNN(Configuration conf, string ns, string addr)
		{
			string key = DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, ns);
			conf.Set(key, addr);
		}
	}
}
