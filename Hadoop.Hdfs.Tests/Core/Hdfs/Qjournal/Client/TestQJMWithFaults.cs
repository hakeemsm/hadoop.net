using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	public class TestQJMWithFaults
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestQJMWithFaults));

		private const string RandSeedProperty = "TestQJMWithFaults.random-seed";

		private const int NumWriterIters = 500;

		private const int SegmentsPerWriter = 2;

		private static readonly Configuration conf = new Configuration();

		static TestQJMWithFaults()
		{
			// Don't retry connections - it just slows down the tests.
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 0);
			// Make tests run faster by avoiding fsync()
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		private static readonly JournalFaultInjector faultInjector = JournalFaultInjector
			.instance = Org.Mockito.Mockito.Mock<JournalFaultInjector>();

		// Set up fault injection mock.
		/// <summary>
		/// Run through the creation of a log without any faults injected,
		/// and count how many RPCs are made to each node.
		/// </summary>
		/// <remarks>
		/// Run through the creation of a log without any faults injected,
		/// and count how many RPCs are made to each node. This sets the
		/// bounds for the other test cases, so they can exhaustively explore
		/// the space of potential failures.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private static long DetermineMaxIpcNumber()
		{
			Configuration conf = new Configuration();
			MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).Build();
			QuorumJournalManager qjm = null;
			long ret;
			try
			{
				qjm = CreateInjectableQJM(cluster);
				qjm.Format(QJMTestUtil.FakeNsinfo);
				DoWorkload(cluster, qjm);
				ICollection<int> ipcCounts = Sets.NewTreeSet();
				foreach (AsyncLogger l in qjm.GetLoggerSetForTests().GetLoggersForTests())
				{
					TestQJMWithFaults.InvocationCountingChannel ch = (TestQJMWithFaults.InvocationCountingChannel
						)l;
					ch.WaitForAllPendingCalls();
					ipcCounts.AddItem(ch.GetRpcCount());
				}
				// All of the loggers should have sent the same number of RPCs, since there
				// were no failures.
				NUnit.Framework.Assert.AreEqual(1, ipcCounts.Count);
				ret = ipcCounts.First();
				Log.Info("Max IPC count = " + ret);
			}
			finally
			{
				IOUtils.CloseStream(qjm);
				cluster.Shutdown();
			}
			return ret;
		}

		/// <summary>
		/// Sets up two of the nodes to each drop a single RPC, at all
		/// possible combinations of RPCs.
		/// </summary>
		/// <remarks>
		/// Sets up two of the nodes to each drop a single RPC, at all
		/// possible combinations of RPCs. This may result in the
		/// active writer failing to write. After this point, a new writer
		/// should be able to recover and continue writing without
		/// data loss.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverAfterDoubleFailures()
		{
			long MaxIpcNumber = DetermineMaxIpcNumber();
			for (int failA = 1; failA <= MaxIpcNumber; failA++)
			{
				for (int failB = 1; failB <= MaxIpcNumber; failB++)
				{
					string injectionStr = "(" + failA + ", " + failB + ")";
					Log.Info("\n\n-------------------------------------------\n" + "Beginning test, failing at "
						 + injectionStr + "\n" + "-------------------------------------------\n\n");
					MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).Build();
					QuorumJournalManager qjm = null;
					try
					{
						qjm = CreateInjectableQJM(cluster);
						qjm.Format(QJMTestUtil.FakeNsinfo);
						IList<AsyncLogger> loggers = qjm.GetLoggerSetForTests().GetLoggersForTests();
						FailIpcNumber(loggers[0], failA);
						FailIpcNumber(loggers[1], failB);
						int lastAckedTxn = DoWorkload(cluster, qjm);
						if (lastAckedTxn < 6)
						{
							Log.Info("Failed after injecting failures at " + injectionStr + ". This is expected since we injected a failure in the "
								 + "majority.");
						}
						qjm.Close();
						qjm = null;
						// Now should be able to recover
						qjm = CreateInjectableQJM(cluster);
						long lastRecoveredTxn = QJMTestUtil.RecoverAndReturnLastTxn(qjm);
						NUnit.Framework.Assert.IsTrue(lastRecoveredTxn >= lastAckedTxn);
						QJMTestUtil.WriteSegment(cluster, qjm, lastRecoveredTxn + 1, 3, true);
					}
					catch (Exception t)
					{
						// Test failure! Rethrow with the test setup info so it can be
						// easily triaged.
						throw new RuntimeException("Test failed with injection: " + injectionStr, t);
					}
					finally
					{
						cluster.Shutdown();
						cluster = null;
						IOUtils.CloseStream(qjm);
						qjm = null;
					}
				}
			}
		}

		/// <summary>
		/// Test case in which three JournalNodes randomly flip flop between
		/// up and down states every time they get an RPC.
		/// </summary>
		/// <remarks>
		/// Test case in which three JournalNodes randomly flip flop between
		/// up and down states every time they get an RPC.
		/// The writer keeps track of the latest ACKed edit, and on every
		/// recovery operation, ensures that it recovers at least to that
		/// point or higher. Since at any given point, a majority of JNs
		/// may be injecting faults, any writer operation is allowed to fail,
		/// so long as the exception message indicates it failed due to injected
		/// faults.
		/// Given a random seed, the test should be entirely deterministic.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRandomized()
		{
			long seed;
			long userSpecifiedSeed = long.GetLong(RandSeedProperty);
			if (userSpecifiedSeed != null)
			{
				Log.Info("Using seed specified in system property");
				seed = userSpecifiedSeed;
				// If the user specifies a seed, then we should gather all the
				// IPC trace information so that debugging is easier. This makes
				// the test run about 25% slower otherwise.
				((Log4JLogger)ProtobufRpcEngine.Log).GetLogger().SetLevel(Level.All);
			}
			else
			{
				seed = new Random().NextLong();
			}
			Log.Info("Random seed: " + seed);
			Random r = new Random(seed);
			MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).Build();
			// Format the cluster using a non-faulty QJM.
			QuorumJournalManager qjmForInitialFormat = CreateInjectableQJM(cluster);
			qjmForInitialFormat.Format(QJMTestUtil.FakeNsinfo);
			qjmForInitialFormat.Close();
			try
			{
				long txid = 0;
				long lastAcked = 0;
				for (int i = 0; i < NumWriterIters; i++)
				{
					Log.Info("Starting writer " + i + "\n-------------------");
					QuorumJournalManager qjm = CreateRandomFaultyQJM(cluster, r);
					try
					{
						long recovered;
						try
						{
							recovered = QJMTestUtil.RecoverAndReturnLastTxn(qjm);
						}
						catch (Exception t)
						{
							Log.Info("Failed recovery", t);
							CheckException(t);
							continue;
						}
						NUnit.Framework.Assert.IsTrue("Recovered only up to txnid " + recovered + " but had gotten an ack for "
							 + lastAcked, recovered >= lastAcked);
						txid = recovered + 1;
						// Periodically purge old data on disk so it's easier to look
						// at failure cases.
						if (txid > 100 && i % 10 == 1)
						{
							qjm.PurgeLogsOlderThan(txid - 100);
						}
						Holder<Exception> thrown = new Holder<Exception>(null);
						for (int j = 0; j < SegmentsPerWriter; j++)
						{
							lastAcked = WriteSegmentUntilCrash(cluster, qjm, txid, 4, thrown);
							if (thrown.held != null)
							{
								Log.Info("Failed write", thrown.held);
								CheckException(thrown.held);
								break;
							}
							txid += 4;
						}
					}
					finally
					{
						qjm.Close();
					}
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private void CheckException(Exception t)
		{
			GenericTestUtils.AssertExceptionContains("Injected", t);
			if (t.ToString().Contains("AssertionError"))
			{
				throw new RuntimeException("Should never see AssertionError in fault test!", t);
			}
		}

		private long WriteSegmentUntilCrash(MiniJournalCluster cluster, QuorumJournalManager
			 qjm, long txid, int numTxns, Holder<Exception> thrown)
		{
			long firstTxId = txid;
			long lastAcked = txid - 1;
			try
			{
				EditLogOutputStream stm = qjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				for (int i = 0; i < numTxns; i++)
				{
					QJMTestUtil.WriteTxns(stm, txid++, 1);
					lastAcked++;
				}
				stm.Close();
				qjm.FinalizeLogSegment(firstTxId, lastAcked);
			}
			catch (Exception t)
			{
				thrown.held = t;
			}
			return lastAcked;
		}

		/// <summary>
		/// Run a simple workload of becoming the active writer and writing
		/// two log segments: 1-3 and 4-6.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static int DoWorkload(MiniJournalCluster cluster, QuorumJournalManager qjm
			)
		{
			int lastAcked = 0;
			try
			{
				qjm.RecoverUnfinalizedSegments();
				QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
				lastAcked = 3;
				QJMTestUtil.WriteSegment(cluster, qjm, 4, 3, true);
				lastAcked = 6;
			}
			catch (QuorumException qe)
			{
				Log.Info("Failed to write at txid " + lastAcked, qe);
			}
			return lastAcked;
		}

		/// <summary>
		/// Inject a failure at the given IPC number, such that the JN never
		/// receives the RPC.
		/// </summary>
		/// <remarks>
		/// Inject a failure at the given IPC number, such that the JN never
		/// receives the RPC. The client side sees an IOException. Future
		/// IPCs after this number will be received as usual.
		/// </remarks>
		private void FailIpcNumber(AsyncLogger logger, int idx)
		{
			((TestQJMWithFaults.InvocationCountingChannel)logger).FailIpcNumber(idx);
		}

		private class RandomFaultyChannel : IPCLoggerChannel
		{
			private readonly Random random;

			private readonly float injectionProbability = 0.1f;

			private bool isUp = true;

			public RandomFaultyChannel(Configuration conf, NamespaceInfo nsInfo, string journalId
				, IPEndPoint addr, long seed)
				: base(conf, nsInfo, journalId, addr)
			{
				this.random = new Random(seed);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override QJournalProtocol CreateProxy()
			{
				QJournalProtocol realProxy = base.CreateProxy();
				return MockProxy(new _WrapEveryCall_351(this, realProxy));
			}

			private sealed class _WrapEveryCall_351 : TestQJMWithFaults.WrapEveryCall<object>
			{
				public _WrapEveryCall_351(RandomFaultyChannel _enclosing, object baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				internal override void BeforeCall(InvocationOnMock invocation)
				{
					if (this._enclosing.random.NextFloat() < this._enclosing.injectionProbability)
					{
						this._enclosing.isUp = !this._enclosing.isUp;
						TestQJMWithFaults.Log.Info("transitioned " + this._enclosing.addr + " to " + (this
							._enclosing.isUp ? "up" : "down"));
					}
					if (!this._enclosing.isUp)
					{
						throw new IOException("Injected - faking being down");
					}
					if (invocation.GetMethod().Name.Equals("acceptRecovery"))
					{
						if (this._enclosing.random.NextFloat() < this._enclosing.injectionProbability)
						{
							Org.Mockito.Mockito.DoThrow(new IOException("Injected - faking fault before persisting paxos data"
								)).When(TestQJMWithFaults.faultInjector).BeforePersistPaxosData();
						}
						else
						{
							if (this._enclosing.random.NextFloat() < this._enclosing.injectionProbability)
							{
								Org.Mockito.Mockito.DoThrow(new IOException("Injected - faking fault after persisting paxos data"
									)).When(TestQJMWithFaults.faultInjector).AfterPersistPaxosData();
							}
						}
					}
				}

				internal override void AfterCall(InvocationOnMock invocation, bool succeeded)
				{
					Org.Mockito.Mockito.Reset(TestQJMWithFaults.faultInjector);
				}

				private readonly RandomFaultyChannel _enclosing;
			}

			protected internal override ExecutorService CreateSingleThreadExecutor()
			{
				return MoreExecutors.SameThreadExecutor();
			}
		}

		private class InvocationCountingChannel : IPCLoggerChannel
		{
			private int rpcCount = 0;

			private readonly IDictionary<int, Callable<Void>> injections = Maps.NewHashMap();

			public InvocationCountingChannel(Configuration conf, NamespaceInfo nsInfo, string
				 journalId, IPEndPoint addr)
				: base(conf, nsInfo, journalId, addr)
			{
			}

			internal virtual int GetRpcCount()
			{
				return rpcCount;
			}

			internal virtual void FailIpcNumber(int idx)
			{
				Preconditions.CheckArgument(idx > 0, "id must be positive");
				Inject(idx, new _Callable_406(idx));
			}

			private sealed class _Callable_406 : Callable<Void>
			{
				public _Callable_406(int idx)
				{
					this.idx = idx;
				}

				/// <exception cref="System.Exception"/>
				public Void Call()
				{
					throw new IOException("injected failed IPC at " + idx);
				}

				private readonly int idx;
			}

			private void Inject(int beforeRpcNumber, Callable<Void> injectedCode)
			{
				injections[beforeRpcNumber] = injectedCode;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override QJournalProtocol CreateProxy()
			{
				QJournalProtocol realProxy = base.CreateProxy();
				QJournalProtocol mock = MockProxy(new _WrapEveryCall_422(this, realProxy));
				return mock;
			}

			private sealed class _WrapEveryCall_422 : TestQJMWithFaults.WrapEveryCall<object>
			{
				public _WrapEveryCall_422(InvocationCountingChannel _enclosing, object baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				internal override void BeforeCall(InvocationOnMock invocation)
				{
					this._enclosing.rpcCount++;
					string callStr = "[" + this._enclosing.addr + "] " + invocation.GetMethod().Name 
						+ "(" + Joiner.On(", ").Join(invocation.GetArguments()) + ")";
					Callable<Void> inject = this._enclosing.injections[this._enclosing.rpcCount];
					if (inject != null)
					{
						TestQJMWithFaults.Log.Info("Injecting code before IPC #" + this._enclosing.rpcCount
							 + ": " + callStr);
						inject.Call();
					}
					else
					{
						TestQJMWithFaults.Log.Info("IPC call #" + this._enclosing.rpcCount + ": " + callStr
							);
					}
				}

				private readonly InvocationCountingChannel _enclosing;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static QJournalProtocol MockProxy(TestQJMWithFaults.WrapEveryCall<object>
			 wrapper)
		{
			QJournalProtocol mock = Org.Mockito.Mockito.Mock<QJournalProtocol>(Org.Mockito.Mockito
				.WithSettings().DefaultAnswer(wrapper).ExtraInterfaces(typeof(IDisposable)));
			return mock;
		}

		private abstract class WrapEveryCall<T> : Org.Mockito.Stubbing.Answer<T>
		{
			private readonly object realObj;

			internal WrapEveryCall(object realObj)
			{
				this.realObj = realObj;
			}

			/// <exception cref="System.Exception"/>
			public virtual T Answer(InvocationOnMock invocation)
			{
				// Don't want to inject an error on close() since that isn't
				// actually an IPC call!
				if (!typeof(IDisposable).Equals(invocation.GetMethod().DeclaringType))
				{
					BeforeCall(invocation);
				}
				bool success = false;
				try
				{
					T ret = (T)invocation.GetMethod().Invoke(realObj, invocation.GetArguments());
					success = true;
					return ret;
				}
				catch (TargetInvocationException ite)
				{
					throw ite.InnerException;
				}
				finally
				{
					AfterCall(invocation, success);
				}
			}

			/// <exception cref="System.Exception"/>
			internal abstract void BeforeCall(InvocationOnMock invocation);

			internal virtual void AfterCall(InvocationOnMock invocation, bool succeeded)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private static QuorumJournalManager CreateInjectableQJM(MiniJournalCluster cluster
			)
		{
			AsyncLogger.Factory spyFactory = new _Factory_487();
			return new QuorumJournalManager(conf, cluster.GetQuorumJournalURI(QJMTestUtil.Jid
				), QJMTestUtil.FakeNsinfo, spyFactory);
		}

		private sealed class _Factory_487 : AsyncLogger.Factory
		{
			public _Factory_487()
			{
			}

			public AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, string 
				journalId, IPEndPoint addr)
			{
				return new TestQJMWithFaults.InvocationCountingChannel(conf, nsInfo, journalId, addr
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private static QuorumJournalManager CreateRandomFaultyQJM(MiniJournalCluster cluster
			, Random seedGenerator)
		{
			AsyncLogger.Factory spyFactory = new _Factory_502(seedGenerator);
			return new QuorumJournalManager(conf, cluster.GetQuorumJournalURI(QJMTestUtil.Jid
				), QJMTestUtil.FakeNsinfo, spyFactory);
		}

		private sealed class _Factory_502 : AsyncLogger.Factory
		{
			public _Factory_502(Random seedGenerator)
			{
				this.seedGenerator = seedGenerator;
			}

			public AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, string 
				journalId, IPEndPoint addr)
			{
				return new TestQJMWithFaults.RandomFaultyChannel(conf, nsInfo, journalId, addr, seedGenerator
					.NextLong());
			}

			private readonly Random seedGenerator;
		}
	}
}
