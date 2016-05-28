using System.IO;
using System.Net;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	public class TestEpochsAreUnique
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestEpochsAreUnique));

		private const string Jid = "testEpochsAreUnique-jid";

		private static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		private readonly Random r = new Random();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleThreaded()
		{
			Configuration conf = new Configuration();
			MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).Build();
			URI uri = cluster.GetQuorumJournalURI(Jid);
			QuorumJournalManager qjm = new QuorumJournalManager(conf, uri, FakeNsinfo);
			try
			{
				qjm.Format(FakeNsinfo);
			}
			finally
			{
				qjm.Close();
			}
			try
			{
				// With no failures or contention, epochs should increase one-by-one
				for (int i = 0; i < 5; i++)
				{
					qjm = new QuorumJournalManager(conf, uri, FakeNsinfo);
					try
					{
						qjm.CreateNewUniqueEpoch();
						NUnit.Framework.Assert.AreEqual(i + 1, qjm.GetLoggerSetForTests().GetEpoch());
					}
					finally
					{
						qjm.Close();
					}
				}
				long prevEpoch = 5;
				// With some failures injected, it should still always increase, perhaps
				// skipping some
				for (int i_1 = 0; i_1 < 20; i_1++)
				{
					long newEpoch = -1;
					while (true)
					{
						qjm = new QuorumJournalManager(conf, uri, FakeNsinfo, new TestEpochsAreUnique.FaultyLoggerFactory
							(this));
						try
						{
							qjm.CreateNewUniqueEpoch();
							newEpoch = qjm.GetLoggerSetForTests().GetEpoch();
							break;
						}
						catch (IOException)
						{
						}
						finally
						{
							// It's OK to fail to create an epoch, since we randomly inject
							// faults. It's possible we'll inject faults in too many of the
							// underlying nodes, and a failure is expected in that case
							qjm.Close();
						}
					}
					Log.Info("Created epoch " + newEpoch);
					NUnit.Framework.Assert.IsTrue("New epoch " + newEpoch + " should be greater than previous "
						 + prevEpoch, newEpoch > prevEpoch);
					prevEpoch = newEpoch;
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private class FaultyLoggerFactory : AsyncLogger.Factory
		{
			public virtual AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, 
				string journalId, IPEndPoint addr)
			{
				AsyncLogger ch = IPCLoggerChannel.Factory.CreateLogger(conf, nsInfo, journalId, addr
					);
				AsyncLogger spy = Org.Mockito.Mockito.Spy(ch);
				Org.Mockito.Mockito.DoAnswer(new TestEpochsAreUnique.SometimesFaulty<long>(this, 
					0.10f)).When(spy).GetJournalState();
				Org.Mockito.Mockito.DoAnswer(new TestEpochsAreUnique.SometimesFaulty<Void>(this, 
					0.40f)).When(spy).NewEpoch(Org.Mockito.Mockito.AnyLong());
				return spy;
			}

			internal FaultyLoggerFactory(TestEpochsAreUnique _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestEpochsAreUnique _enclosing;
		}

		private class SometimesFaulty<T> : Org.Mockito.Stubbing.Answer<ListenableFuture<T
			>>
		{
			private readonly float faultProbability;

			public SometimesFaulty(TestEpochsAreUnique _enclosing, float faultProbability)
			{
				this._enclosing = _enclosing;
				this.faultProbability = faultProbability;
			}

			/// <exception cref="System.Exception"/>
			public virtual ListenableFuture<T> Answer(InvocationOnMock invocation)
			{
				if (this._enclosing.r.NextFloat() < this.faultProbability)
				{
					return Futures.ImmediateFailedFuture(new IOException("Injected fault"));
				}
				return (ListenableFuture<T>)invocation.CallRealMethod();
			}

			private readonly TestEpochsAreUnique _enclosing;
		}
	}
}
