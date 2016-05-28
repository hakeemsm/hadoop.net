using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Internal.Stubbing.Answers;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// These tests make sure that DFSClient retries fetching data from DFS
	/// properly in case of errors.
	/// </summary>
	public class TestDFSClientRetries
	{
		private const string Address = "0.0.0.0";

		private const int PingInterval = 1000;

		private const int MinSleepTime = 1000;

		public static readonly Log Log = LogFactory.GetLog(typeof(TestDFSClientRetries).FullName
			);

		private static Configuration conf = null;

		private class TestServer : Server
		{
			private bool sleep;

			private Type responseClass;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: this(handlerCount, sleep, typeof(LongWritable), null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep, Type paramClass, Type responseClass
				)
				: base(Address, 0, paramClass, handlerCount, conf)
			{
				this.sleep = sleep;
				this.responseClass = responseClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Writable Call(RPC.RpcKind rpcKind, string protocol, Writable param
				, long receiveTime)
			{
				if (sleep)
				{
					// sleep a bit
					try
					{
						Sharpen.Thread.Sleep(PingInterval + MinSleepTime);
					}
					catch (Exception)
					{
					}
				}
				if (responseClass != null)
				{
					try
					{
						return System.Activator.CreateInstance(responseClass);
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				else
				{
					return param;
				}
			}
			// echo param as result
		}

		// writes 'len' bytes of data to out.
		/// <exception cref="System.IO.IOException"/>
		private static void WriteData(OutputStream @out, int len)
		{
			byte[] buf = new byte[4096 * 16];
			while (len > 0)
			{
				int toWrite = Math.Min(len, buf.Length);
				@out.Write(buf, 0, toWrite);
				len -= toWrite;
			}
		}

		[SetUp]
		public virtual void SetupConf()
		{
			conf = new HdfsConfiguration();
		}

		/// <summary>
		/// This makes sure that when DN closes clients socket after client had
		/// successfully connected earlier, the data can still be fetched.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteTimeoutAtDataNode()
		{
			int writeTimeout = 100;
			//milliseconds.
			// set a very short write timeout for datanode, so that tests runs fast.
			conf.SetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey, writeTimeout);
			// set a smaller block size
			int blockSize = 10 * 1024 * 1024;
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetInt(DFSConfigKeys.DfsClientMaxBlockAcquireFailuresKey, 1);
			// set a small buffer size
			int bufferSize = 4096;
			conf.SetInt(CommonConfigurationKeys.IoFileBufferSizeKey, bufferSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path filePath = new Path("/testWriteTimeoutAtDataNode");
				OutputStream @out = fs.Create(filePath, true, bufferSize);
				// write a 2 block file.
				WriteData(@out, 2 * blockSize);
				@out.Close();
				byte[] buf = new byte[1024 * 1024];
				// enough to empty TCP buffers.
				InputStream @in = fs.Open(filePath, bufferSize);
				//first read a few bytes
				IOUtils.ReadFully(@in, buf, 0, bufferSize / 2);
				//now read few more chunks of data by sleeping in between :
				for (int i = 0; i < 10; i++)
				{
					Sharpen.Thread.Sleep(2 * writeTimeout);
					// force write timeout at the datanode.
					// read enough to empty out socket buffers.
					IOUtils.ReadFully(@in, buf, 0, buf.Length);
				}
				// successfully read with write timeout on datanodes.
				@in.Close();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		// more tests related to different failure cases can be added here.
		/// <summary>
		/// Verify that client will correctly give up after the specified number
		/// of times trying to add a block
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNotYetReplicatedErrors()
		{
			string exceptionMsg = "Nope, not replicated yet...";
			int maxRetries = 1;
			// Allow one retry (total of two calls)
			conf.SetInt(DFSConfigKeys.DfsClientBlockWriteLocatefollowingblockRetriesKey, maxRetries
				);
			NamenodeProtocols mockNN = Org.Mockito.Mockito.Mock<NamenodeProtocols>();
			Answer<object> answer = new _ThrowsException_237(maxRetries, exceptionMsg, new IOException
				());
			// First call was not a retry
			Org.Mockito.Mockito.When(mockNN.AddBlock(Matchers.AnyString(), Matchers.AnyString
				(), Matchers.Any<ExtendedBlock>(), Matchers.Any<DatanodeInfo[]>(), Matchers.AnyLong
				(), Matchers.Any<string[]>())).ThenAnswer(answer);
			Org.Mockito.Mockito.DoReturn(new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission
				((short)777), "owner", "group", new byte[0], new byte[0], 1010, 0, null, unchecked(
				(byte)0))).When(mockNN).GetFileInfo(Matchers.AnyString());
			Org.Mockito.Mockito.DoReturn(new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission
				((short)777), "owner", "group", new byte[0], new byte[0], 1010, 0, null, unchecked(
				(byte)0))).When(mockNN).Create(Matchers.AnyString(), (FsPermission)Matchers.AnyObject
				(), Matchers.AnyString(), (EnumSetWritable<CreateFlag>)Matchers.AnyObject(), Matchers.AnyBoolean
				(), Matchers.AnyShort(), Matchers.AnyLong(), (CryptoProtocolVersion[])Matchers.AnyObject
				());
			DFSClient client = new DFSClient(null, mockNN, conf, null);
			OutputStream os = client.Create("testfile", true);
			os.Write(20);
			// write one random byte
			try
			{
				os.Close();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Retries are not being stopped correctly: " + e.Message
					, e.Message.Equals(exceptionMsg));
			}
		}

		private sealed class _ThrowsException_237 : ThrowsException
		{
			public _ThrowsException_237(int maxRetries, string exceptionMsg, Exception baseArg1
				)
				: base(baseArg1)
			{
				this.maxRetries = maxRetries;
				this.exceptionMsg = exceptionMsg;
				this.retryCount = 0;
			}

			internal int retryCount;

			/// <exception cref="System.Exception"/>
			public override object Answer(InvocationOnMock invocation)
			{
				this.retryCount++;
				System.Console.Out.WriteLine("addBlock has been called " + this.retryCount + " times"
					);
				if (this.retryCount > maxRetries + 1)
				{
					throw new IOException("Retried too many times: " + this.retryCount);
				}
				else
				{
					throw new RemoteException(typeof(NotReplicatedYetException).FullName, exceptionMsg
						);
				}
			}

			private readonly int maxRetries;

			private readonly string exceptionMsg;
		}

		/// <summary>
		/// This tests that DFSInputStream failures are counted for a given read
		/// operation, and not over the lifetime of the stream.
		/// </summary>
		/// <remarks>
		/// This tests that DFSInputStream failures are counted for a given read
		/// operation, and not over the lifetime of the stream. It is a regression
		/// test for HDFS-127.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailuresArePerOperation()
		{
			long fileSize = 4096;
			Path file = new Path("/testFile");
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 2 * 1000);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				NamenodeProtocols preSpyNN = cluster.GetNameNodeRpc();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(preSpyNN);
				DFSClient client = new DFSClient(null, spyNN, conf, null);
				int maxBlockAcquires = client.GetMaxBlockAcquireFailures();
				NUnit.Framework.Assert.IsTrue(maxBlockAcquires > 0);
				DFSTestUtil.CreateFile(fs, file, fileSize, (short)1, 12345L);
				/*seed*/
				// If the client will retry maxBlockAcquires times, then if we fail
				// any more than that number of times, the operation should entirely
				// fail.
				Org.Mockito.Mockito.DoAnswer(new TestDFSClientRetries.FailNTimesAnswer(preSpyNN, 
					maxBlockAcquires + 1)).When(spyNN).GetBlockLocations(Matchers.AnyString(), Matchers.AnyLong
					(), Matchers.AnyLong());
				try
				{
					IOUtils.CopyBytes(client.Open(file.ToString()), new IOUtils.NullOutputStream(), conf
						, true);
					NUnit.Framework.Assert.Fail("Didn't get exception");
				}
				catch (IOException ioe)
				{
					DFSClient.Log.Info("Got expected exception", ioe);
				}
				// If we fail exactly that many times, then it should succeed.
				Org.Mockito.Mockito.DoAnswer(new TestDFSClientRetries.FailNTimesAnswer(preSpyNN, 
					maxBlockAcquires)).When(spyNN).GetBlockLocations(Matchers.AnyString(), Matchers.AnyLong
					(), Matchers.AnyLong());
				IOUtils.CopyBytes(client.Open(file.ToString()), new IOUtils.NullOutputStream(), conf
					, true);
				DFSClient.Log.Info("Starting test case for failure reset");
				// Now the tricky case - if we fail a few times on one read, then succeed,
				// then fail some more on another read, it shouldn't fail.
				Org.Mockito.Mockito.DoAnswer(new TestDFSClientRetries.FailNTimesAnswer(preSpyNN, 
					maxBlockAcquires)).When(spyNN).GetBlockLocations(Matchers.AnyString(), Matchers.AnyLong
					(), Matchers.AnyLong());
				DFSInputStream @is = client.Open(file.ToString());
				byte[] buf = new byte[10];
				IOUtils.ReadFully(@is, buf, 0, buf.Length);
				DFSClient.Log.Info("First read successful after some failures.");
				// Further reads at this point will succeed since it has the good block locations.
				// So, force the block locations on this stream to be refreshed from bad info.
				// When reading again, it should start from a fresh failure count, since
				// we're starting a new operation on the user level.
				Org.Mockito.Mockito.DoAnswer(new TestDFSClientRetries.FailNTimesAnswer(preSpyNN, 
					maxBlockAcquires)).When(spyNN).GetBlockLocations(Matchers.AnyString(), Matchers.AnyLong
					(), Matchers.AnyLong());
				@is.OpenInfo();
				// Seek to beginning forces a reopen of the BlockReader - otherwise it'll
				// just keep reading on the existing stream and the fact that we've poisoned
				// the block info won't do anything.
				@is.Seek(0);
				IOUtils.ReadFully(@is, buf, 0, buf.Length);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test DFSClient can continue to function after renewLease RPC
		/// receives SocketTimeoutException.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseRenewSocketTimeout()
		{
			string file1 = "/testFile1";
			string file2 = "/testFile2";
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 2 * 1000);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(cluster.GetNameNodeRpc());
				Org.Mockito.Mockito.DoThrow(new SocketTimeoutException()).When(spyNN).RenewLease(
					Org.Mockito.Mockito.AnyString());
				DFSClient client = new DFSClient(null, spyNN, conf, null);
				// Get hold of the lease renewer instance used by the client
				LeaseRenewer leaseRenewer = client.GetLeaseRenewer();
				leaseRenewer.SetRenewalTime(100);
				OutputStream out1 = client.Create(file1, false);
				Org.Mockito.Mockito.Verify(spyNN, Org.Mockito.Mockito.Timeout(10000).Times(1)).RenewLease
					(Org.Mockito.Mockito.AnyString());
				VerifyEmptyLease(leaseRenewer);
				try
				{
					out1.Write(new byte[256]);
					NUnit.Framework.Assert.Fail("existing output stream should be aborted");
				}
				catch (IOException)
				{
				}
				// Verify DFSClient can do read operation after renewLease aborted.
				client.Exists(file2);
				// Verify DFSClient can do write operation after renewLease no longer
				// throws SocketTimeoutException.
				Org.Mockito.Mockito.DoNothing().When(spyNN).RenewLease(Org.Mockito.Mockito.AnyString
					());
				leaseRenewer = client.GetLeaseRenewer();
				leaseRenewer.SetRenewalTime(100);
				OutputStream out2 = client.Create(file2, false);
				Org.Mockito.Mockito.Verify(spyNN, Org.Mockito.Mockito.Timeout(10000).Times(2)).RenewLease
					(Org.Mockito.Mockito.AnyString());
				out2.Write(new byte[256]);
				out2.Close();
				VerifyEmptyLease(leaseRenewer);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test that getAdditionalBlock() and close() are idempotent.</summary>
		/// <remarks>
		/// Test that getAdditionalBlock() and close() are idempotent. This allows
		/// a client to safely retry a call and still produce a correct
		/// file. See HDFS-3031.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIdempotentAllocateBlockAndClose()
		{
			string src = "/testIdempotentAllocateBlock";
			Path file = new Path(src);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 4096);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				NamenodeProtocols preSpyNN = cluster.GetNameNodeRpc();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(preSpyNN);
				DFSClient client = new DFSClient(null, spyNN, conf, null);
				// Make the call to addBlock() get called twice, as if it were retried
				// due to an IPC issue.
				Org.Mockito.Mockito.DoAnswer(new _Answer_436(cluster, src)).When(spyNN).AddBlock(
					Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
					.Any<ExtendedBlock>(), Org.Mockito.Mockito.Any<DatanodeInfo[]>(), Org.Mockito.Mockito
					.AnyLong(), Org.Mockito.Mockito.Any<string[]>());
				// Retrying should result in a new block at the end of the file.
				// (abandoning the old one)
				// We shouldn't have gained an extra block by the RPC.
				Org.Mockito.Mockito.DoAnswer(new _Answer_459()).When(spyNN).Complete(Org.Mockito.Mockito
					.AnyString(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.Any<ExtendedBlock
					>(), Matchers.AnyLong());
				// complete() may return false a few times before it returns
				// true. We want to wait until it returns true, and then
				// make it retry one more time after that.
				// We got a successful close. Call it again to check idempotence.
				OutputStream stm = client.Create(file.ToString(), true);
				try
				{
					AppendTestUtil.Write(stm, 0, 10000);
					stm.Close();
					stm = null;
				}
				finally
				{
					IOUtils.Cleanup(Log, stm);
				}
				// Make sure the mock was actually properly injected.
				Org.Mockito.Mockito.Verify(spyNN, Org.Mockito.Mockito.AtLeastOnce()).AddBlock(Org.Mockito.Mockito
					.AnyString(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.Any<ExtendedBlock
					>(), Org.Mockito.Mockito.Any<DatanodeInfo[]>(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
					.Any<string[]>());
				Org.Mockito.Mockito.Verify(spyNN, Org.Mockito.Mockito.AtLeastOnce()).Complete(Org.Mockito.Mockito
					.AnyString(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.Any<ExtendedBlock
					>(), Matchers.AnyLong());
				AppendTestUtil.Check(fs, file, 10000);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Answer_436 : Answer<LocatedBlock>
		{
			public _Answer_436(MiniDFSCluster cluster, string src)
			{
				this.cluster = cluster;
				this.src = src;
			}

			/// <exception cref="System.Exception"/>
			public LocatedBlock Answer(InvocationOnMock invocation)
			{
				LocatedBlock ret = (LocatedBlock)invocation.CallRealMethod();
				LocatedBlocks lb = cluster.GetNameNodeRpc().GetBlockLocations(src, 0, long.MaxValue
					);
				int blockCount = lb.GetLocatedBlocks().Count;
				NUnit.Framework.Assert.AreEqual(lb.GetLastLocatedBlock().GetBlock(), ret.GetBlock
					());
				LocatedBlock ret2 = (LocatedBlock)invocation.CallRealMethod();
				lb = cluster.GetNameNodeRpc().GetBlockLocations(src, 0, long.MaxValue);
				int blockCount2 = lb.GetLocatedBlocks().Count;
				NUnit.Framework.Assert.AreEqual(lb.GetLastLocatedBlock().GetBlock(), ret2.GetBlock
					());
				NUnit.Framework.Assert.AreEqual(blockCount, blockCount2);
				return ret2;
			}

			private readonly MiniDFSCluster cluster;

			private readonly string src;
		}

		private sealed class _Answer_459 : Answer<bool>
		{
			public _Answer_459()
			{
			}

			/// <exception cref="System.Exception"/>
			public bool Answer(InvocationOnMock invocation)
			{
				TestDFSClientRetries.Log.Info("Called complete(: " + Joiner.On(",").Join(invocation
					.GetArguments()) + ")");
				if (!(bool)invocation.CallRealMethod())
				{
					TestDFSClientRetries.Log.Info("Complete call returned false, not faking a retry RPC"
						);
					return false;
				}
				try
				{
					bool ret = (bool)invocation.CallRealMethod();
					TestDFSClientRetries.Log.Info("Complete call returned true, faked second RPC. " +
						 "Returned: " + ret);
					return ret;
				}
				catch (Exception t)
				{
					TestDFSClientRetries.Log.Error("Idempotent retry threw exception", t);
					throw;
				}
			}
		}

		/// <summary>
		/// Mock Answer implementation of NN.getBlockLocations that will return
		/// a poisoned block list a certain number of times before returning
		/// a proper one.
		/// </summary>
		private class FailNTimesAnswer : Org.Mockito.Stubbing.Answer<LocatedBlocks>
		{
			private int failuresLeft;

			private readonly NamenodeProtocols realNN;

			public FailNTimesAnswer(NamenodeProtocols preSpyNN, int timesToFail)
			{
				failuresLeft = timesToFail;
				this.realNN = preSpyNN;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual LocatedBlocks Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				LocatedBlocks realAnswer = realNN.GetBlockLocations((string)args[0], (long)args[1
					], (long)args[2]);
				if (failuresLeft-- > 0)
				{
					NameNode.Log.Info("FailNTimesAnswer injecting failure.");
					return MakeBadBlockList(realAnswer);
				}
				NameNode.Log.Info("FailNTimesAnswer no longer failing.");
				return realAnswer;
			}

			private LocatedBlocks MakeBadBlockList(LocatedBlocks goodBlockList)
			{
				LocatedBlock goodLocatedBlock = goodBlockList.Get(0);
				LocatedBlock badLocatedBlock = new LocatedBlock(goodLocatedBlock.GetBlock(), new 
					DatanodeInfo[] { DFSTestUtil.GetDatanodeInfo("1.2.3.4", "bogus", 1234) }, goodLocatedBlock
					.GetStartOffset(), false);
				IList<LocatedBlock> badBlocks = new AList<LocatedBlock>();
				badBlocks.AddItem(badLocatedBlock);
				return new LocatedBlocks(goodBlockList.GetFileLength(), false, badBlocks, null, true
					, null);
			}
		}

		/// <summary>Test that a DFSClient waits for random time before retry on busy blocks.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSClientRetriesOnBusyBlocks()
		{
			System.Console.Out.WriteLine("Testing DFSClient random waiting on busy blocks.");
			//
			// Test settings: 
			// 
			//           xcievers    fileLen   #clients  timeWindow    #retries
			//           ========    =======   ========  ==========    ========
			// Test 1:          2       6 MB         50      300 ms           3
			// Test 2:          2       6 MB         50      300 ms          50
			// Test 3:          2       6 MB         50     1000 ms           3
			// Test 4:          2       6 MB         50     1000 ms          50
			// 
			//   Minimum xcievers is 2 since 1 thread is reserved for registry.
			//   Test 1 & 3 may fail since # retries is low. 
			//   Test 2 & 4 should never fail since (#threads)/(xcievers-1) is the upper
			//   bound for guarantee to not throw BlockMissingException.
			//
			int xcievers = 2;
			int fileLen = 6 * 1024 * 1024;
			int threads = 50;
			int retries = 3;
			int timeWin = 300;
			//
			// Test 1: might fail
			// 
			long timestamp = Time.Now();
			bool pass = BusyTest(xcievers, threads, fileLen, timeWin, retries);
			long timestamp2 = Time.Now();
			if (pass)
			{
				Log.Info("Test 1 succeeded! Time spent: " + (timestamp2 - timestamp) / 1000.0 + " sec."
					);
			}
			else
			{
				Log.Warn("Test 1 failed, but relax. Time spent: " + (timestamp2 - timestamp) / 1000.0
					 + " sec.");
			}
			//
			// Test 2: should never fail
			// 
			retries = 50;
			timestamp = Time.Now();
			pass = BusyTest(xcievers, threads, fileLen, timeWin, retries);
			timestamp2 = Time.Now();
			NUnit.Framework.Assert.IsTrue("Something wrong! Test 2 got Exception with maxmum retries!"
				, pass);
			Log.Info("Test 2 succeeded! Time spent: " + (timestamp2 - timestamp) / 1000.0 + " sec."
				);
			//
			// Test 3: might fail
			// 
			retries = 3;
			timeWin = 1000;
			timestamp = Time.Now();
			pass = BusyTest(xcievers, threads, fileLen, timeWin, retries);
			timestamp2 = Time.Now();
			if (pass)
			{
				Log.Info("Test 3 succeeded! Time spent: " + (timestamp2 - timestamp) / 1000.0 + " sec."
					);
			}
			else
			{
				Log.Warn("Test 3 failed, but relax. Time spent: " + (timestamp2 - timestamp) / 1000.0
					 + " sec.");
			}
			//
			// Test 4: should never fail
			//
			retries = 50;
			timeWin = 1000;
			timestamp = Time.Now();
			pass = BusyTest(xcievers, threads, fileLen, timeWin, retries);
			timestamp2 = Time.Now();
			NUnit.Framework.Assert.IsTrue("Something wrong! Test 4 got Exception with maxmum retries!"
				, pass);
			Log.Info("Test 4 succeeded! Time spent: " + (timestamp2 - timestamp) / 1000.0 + " sec."
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool BusyTest(int xcievers, int threads, int fileLen, int timeWin, int retries
			)
		{
			bool ret = true;
			short replicationFactor = 1;
			long blockSize = 128 * 1024 * 1024;
			// DFS block size
			int bufferSize = 4096;
			conf.SetInt(DFSConfigKeys.DfsDatanodeMaxReceiverThreadsKey, xcievers);
			conf.SetInt(DFSConfigKeys.DfsClientMaxBlockAcquireFailuresKey, retries);
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, timeWin);
			// Disable keepalive
			conf.SetInt(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveKey, 0);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(replicationFactor
				).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			Path file1 = new Path("test_data.dat");
			file1 = file1.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory());
			// make URI hdfs://
			try
			{
				FSDataOutputStream stm = fs.Create(file1, true, bufferSize, replicationFactor, blockSize
					);
				// verify that file exists in FS namespace
				NUnit.Framework.Assert.IsTrue(file1 + " should be a file", fs.GetFileStatus(file1
					).IsFile());
				System.Console.Out.WriteLine("Path : \"" + file1 + "\"");
				Log.Info("Path : \"" + file1 + "\"");
				// write 1 block to file
				byte[] buffer = AppendTestUtil.RandomBytes(Time.Now(), fileLen);
				stm.Write(buffer, 0, fileLen);
				stm.Close();
				// verify that file size has changed to the full size
				long len = fs.GetFileStatus(file1).GetLen();
				NUnit.Framework.Assert.IsTrue(file1 + " should be of size " + fileLen + " but found to be of size "
					 + len, len == fileLen);
				// read back and check data integrigy
				byte[] read_buf = new byte[fileLen];
				InputStream @in = fs.Open(file1, fileLen);
				IOUtils.ReadFully(@in, read_buf, 0, fileLen);
				System.Diagnostics.Debug.Assert((Arrays.Equals(buffer, read_buf)));
				@in.Close();
				read_buf = null;
				// GC it if needed
				// compute digest of the content to reduce memory space
				MessageDigest m = MessageDigest.GetInstance("SHA");
				m.Update(buffer, 0, fileLen);
				byte[] hash_sha = m.Digest();
				// spawn multiple threads and all trying to access the same block
				Sharpen.Thread[] readers = new Sharpen.Thread[threads];
				TestDFSClientRetries.Counter counter = new TestDFSClientRetries.Counter(this, 0);
				for (int i = 0; i < threads; ++i)
				{
					TestDFSClientRetries.DFSClientReader reader = new TestDFSClientRetries.DFSClientReader
						(this, file1, cluster, hash_sha, fileLen, counter);
					readers[i] = new Sharpen.Thread(reader);
					readers[i].Start();
				}
				// wait for them to exit
				for (int i_1 = 0; i_1 < threads; ++i_1)
				{
					readers[i_1].Join();
				}
				if (counter.Get() == threads)
				{
					ret = true;
				}
				else
				{
					ret = false;
				}
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Thread got InterruptedException.");
				Sharpen.Runtime.PrintStackTrace(e);
				ret = false;
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				ret = false;
			}
			finally
			{
				fs.Delete(file1, false);
				cluster.Shutdown();
			}
			return ret;
		}

		/// <exception cref="System.Exception"/>
		private void VerifyEmptyLease(LeaseRenewer leaseRenewer)
		{
			int sleepCount = 0;
			while (!leaseRenewer.IsEmpty() && sleepCount++ < 20)
			{
				Sharpen.Thread.Sleep(500);
			}
			NUnit.Framework.Assert.IsTrue("Lease should be empty.", leaseRenewer.IsEmpty());
		}

		internal class DFSClientReader : Runnable
		{
			internal DFSClient client;

			internal readonly Configuration conf;

			internal readonly byte[] expected_sha;

			internal FileSystem fs;

			internal readonly Path filePath;

			internal readonly MiniDFSCluster cluster;

			internal readonly int len;

			internal readonly TestDFSClientRetries.Counter counter;

			internal DFSClientReader(TestDFSClientRetries _enclosing, Path file, MiniDFSCluster
				 cluster, byte[] hash_sha, int fileLen, TestDFSClientRetries.Counter cnt)
			{
				this._enclosing = _enclosing;
				this.filePath = file;
				this.cluster = cluster;
				this.counter = cnt;
				this.len = fileLen;
				this.conf = new HdfsConfiguration();
				this.expected_sha = hash_sha;
				try
				{
					cluster.WaitActive();
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			public virtual void Run()
			{
				try
				{
					this.fs = this.cluster.GetNewFileSystemInstance(0);
					int bufferSize = this.len;
					byte[] buf = new byte[bufferSize];
					InputStream @in = this.fs.Open(this.filePath, bufferSize);
					// read the whole file
					IOUtils.ReadFully(@in, buf, 0, bufferSize);
					// compare with the expected input
					MessageDigest m = MessageDigest.GetInstance("SHA");
					m.Update(buf, 0, bufferSize);
					byte[] hash_sha = m.Digest();
					buf = null;
					// GC if needed since there may be too many threads
					@in.Close();
					this.fs.Close();
					NUnit.Framework.Assert.IsTrue("hashed keys are not the same size", hash_sha.Length
						 == this.expected_sha.Length);
					NUnit.Framework.Assert.IsTrue("hashed keys are not equal", Arrays.Equals(hash_sha
						, this.expected_sha));
					this.counter.Inc();
					// count this thread as successful
					TestDFSClientRetries.Log.Info("Thread correctly read the block.");
				}
				catch (BlockMissingException e)
				{
					TestDFSClientRetries.Log.Info("Bad - BlockMissingException is caught.");
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly TestDFSClientRetries _enclosing;
		}

		internal class Counter
		{
			internal int counter;

			internal Counter(TestDFSClientRetries _enclosing, int n)
			{
				this._enclosing = _enclosing;
				this.counter = n;
			}

			public virtual void Inc()
			{
				lock (this)
				{
					++this.counter;
				}
			}

			public virtual int Get()
			{
				return this.counter;
			}

			private readonly TestDFSClientRetries _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileChecksum()
		{
			string f = "/testGetFileChecksum";
			Path p = new Path(f);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				//create a file
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, p, 1L << 20, (short)3, 20100402L);
				//get checksum
				FileChecksum cs1 = fs.GetFileChecksum(p);
				NUnit.Framework.Assert.IsTrue(cs1 != null);
				//stop the first datanode
				IList<LocatedBlock> locatedblocks = DFSClient.CallGetBlockLocations(cluster.GetNameNodeRpc
					(), f, 0, long.MaxValue).GetLocatedBlocks();
				DatanodeInfo first = locatedblocks[0].GetLocations()[0];
				cluster.StopDataNode(first.GetXferAddr());
				//get checksum again
				FileChecksum cs2 = fs.GetFileChecksum(p);
				NUnit.Framework.Assert.AreEqual(cs1, cs2);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test that timeout occurs when DN does not respond to RPC.</summary>
		/// <remarks>
		/// Test that timeout occurs when DN does not respond to RPC.
		/// Start up a server and ask it to sleep for n seconds. Make an
		/// RPC to the server and set rpcTimeout to less than n and ensure
		/// that socketTimeoutException is obtained
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientDNProtocolTimeout()
		{
			Org.Apache.Hadoop.Ipc.Server server = new TestDFSClientRetries.TestServer(1, true
				);
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			DatanodeID fakeDnId = DFSTestUtil.GetLocalDatanodeID(addr.Port);
			ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
			LocatedBlock fakeBlock = new LocatedBlock(b, new DatanodeInfo[0]);
			ClientDatanodeProtocol proxy = null;
			try
			{
				proxy = DFSUtil.CreateClientDatanodeProtocolProxy(fakeDnId, conf, 500, false, fakeBlock
					);
				proxy.GetReplicaVisibleLength(new ExtendedBlock("bpid", 1));
				NUnit.Framework.Assert.Fail("Did not get expected exception: SocketTimeoutException"
					);
			}
			catch (SocketTimeoutException)
			{
				Log.Info("Got the expected Exception: SocketTimeoutException");
			}
			finally
			{
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				server.Stop();
			}
		}

		/// <summary>Test client retry with namenode restarting.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNamenodeRestart()
		{
			NamenodeRestartTest(new Configuration(), false);
		}

		/// <exception cref="System.Exception"/>
		public static void NamenodeRestartTest(Configuration conf, bool isWebHDFS)
		{
			((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			IList<Exception> exceptions = new AList<Exception>();
			Path dir = new Path("/testNamenodeRestart");
			if (isWebHDFS)
			{
				conf.SetBoolean(DFSConfigKeys.DfsHttpClientRetryPolicyEnabledKey, true);
			}
			else
			{
				conf.SetBoolean(DFSConfigKeys.DfsClientRetryPolicyEnabledKey, true);
			}
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeMinDatanodesKey, 1);
			conf.SetInt(MiniDFSCluster.DfsNamenodeSafemodeExtensionTestingKey, 5000);
			short numDatanodes = 3;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FileSystem fs = isWebHDFS ? WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme) : dfs;
				URI uri = dfs.GetUri();
				NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				//create a file
				long length = 1L << 20;
				Path file1 = new Path(dir, "foo");
				DFSTestUtil.CreateFile(fs, file1, length, numDatanodes, 20120406L);
				//get file status
				FileStatus s1 = fs.GetFileStatus(file1);
				NUnit.Framework.Assert.AreEqual(length, s1.GetLen());
				//create file4, write some data but not close
				Path file4 = new Path(dir, "file4");
				FSDataOutputStream out4 = fs.Create(file4, false, 4096, fs.GetDefaultReplication(
					file4), 1024L, null);
				byte[] bytes = new byte[1000];
				new Random().NextBytes(bytes);
				out4.Write(bytes);
				out4.Write(bytes);
				if (isWebHDFS)
				{
					// WebHDFS does not support hflush. To avoid DataNode communicating with
					// NN while we're shutting down NN, we call out4.close() to finish
					// writing the data
					out4.Close();
				}
				else
				{
					out4.Hflush();
				}
				//shutdown namenode
				NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				cluster.ShutdownNameNode(0);
				NUnit.Framework.Assert.IsFalse(HdfsUtils.IsHealthy(uri));
				//namenode is down, continue writing file4 in a thread
				Sharpen.Thread file4thread = new Sharpen.Thread(new _Runnable_941(isWebHDFS, out4
					, bytes, exceptions));
				//write some more data and then close the file
				file4thread.Start();
				//namenode is down, read the file in a thread
				Sharpen.Thread reader = new Sharpen.Thread(new _Runnable_960(conf, isWebHDFS, file1
					, s1, exceptions));
				//it should retry till namenode is up.
				reader.Start();
				//namenode is down, create another file in a thread
				Path file3 = new Path(dir, "file");
				Sharpen.Thread thread = new Sharpen.Thread(new _Runnable_980(conf, isWebHDFS, file3
					, length, numDatanodes, exceptions));
				//it should retry till namenode is up.
				thread.Start();
				//restart namenode in a new thread
				new Sharpen.Thread(new _Runnable_995(uri, cluster, exceptions)).Start();
				//sleep, restart, and then wait active
				//namenode is down, it should retry until namenode is up again. 
				FileStatus s2 = fs.GetFileStatus(file1);
				NUnit.Framework.Assert.AreEqual(s1, s2);
				//check file1 and file3
				thread.Join();
				AssertEmpty(exceptions);
				NUnit.Framework.Assert.AreEqual(s1.GetLen(), fs.GetFileStatus(file3).GetLen());
				NUnit.Framework.Assert.AreEqual(fs.GetFileChecksum(file1), fs.GetFileChecksum(file3
					));
				reader.Join();
				AssertEmpty(exceptions);
				//check file4
				file4thread.Join();
				AssertEmpty(exceptions);
				{
					FSDataInputStream @in = fs.Open(file4);
					int count = 0;
					for (int r; (r = @in.Read()) != -1; count++)
					{
						NUnit.Framework.Assert.AreEqual(string.Format("count=%d", count), bytes[count % bytes
							.Length], unchecked((byte)r));
					}
					if (!isWebHDFS)
					{
						NUnit.Framework.Assert.AreEqual(5 * bytes.Length, count);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(2 * bytes.Length, count);
					}
					@in.Close();
				}
				//enter safe mode
				NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				NUnit.Framework.Assert.IsFalse(HdfsUtils.IsHealthy(uri));
				//leave safe mode in a new thread
				new Sharpen.Thread(new _Runnable_1048(uri, dfs, exceptions)).Start();
				//sleep and then leave safe mode
				//namenode is in safe mode, create should retry until it leaves safe mode.
				Path file2 = new Path(dir, "bar");
				DFSTestUtil.CreateFile(fs, file2, length, numDatanodes, 20120406L);
				NUnit.Framework.Assert.AreEqual(fs.GetFileChecksum(file1), fs.GetFileChecksum(file2
					));
				NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				//make sure it won't retry on exceptions like FileNotFoundException
				Path nonExisting = new Path(dir, "nonExisting");
				Log.Info("setPermission: " + nonExisting);
				try
				{
					fs.SetPermission(nonExisting, new FsPermission((short)0));
					NUnit.Framework.Assert.Fail();
				}
				catch (FileNotFoundException fnfe)
				{
					Log.Info("GOOD!", fnfe);
				}
				AssertEmpty(exceptions);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Runnable_941 : Runnable
		{
			public _Runnable_941(bool isWebHDFS, FSDataOutputStream out4, byte[] bytes, IList
				<Exception> exceptions)
			{
				this.isWebHDFS = isWebHDFS;
				this.out4 = out4;
				this.bytes = bytes;
				this.exceptions = exceptions;
			}

			public void Run()
			{
				try
				{
					if (!isWebHDFS)
					{
						out4.Write(bytes);
						out4.Write(bytes);
						out4.Write(bytes);
						out4.Close();
					}
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly bool isWebHDFS;

			private readonly FSDataOutputStream out4;

			private readonly byte[] bytes;

			private readonly IList<Exception> exceptions;
		}

		private sealed class _Runnable_960 : Runnable
		{
			public _Runnable_960(Configuration conf, bool isWebHDFS, Path file1, FileStatus s1
				, IList<Exception> exceptions)
			{
				this.conf = conf;
				this.isWebHDFS = isWebHDFS;
				this.file1 = file1;
				this.s1 = s1;
				this.exceptions = exceptions;
			}

			public void Run()
			{
				try
				{
					FileSystem fs = TestDFSClientRetries.CreateFsWithDifferentUsername(conf, isWebHDFS
						);
					FSDataInputStream @in = fs.Open(file1);
					int count = 0;
					for (; @in.Read() != -1; count++)
					{
					}
					@in.Close();
					NUnit.Framework.Assert.AreEqual(s1.GetLen(), count);
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly Configuration conf;

			private readonly bool isWebHDFS;

			private readonly Path file1;

			private readonly FileStatus s1;

			private readonly IList<Exception> exceptions;
		}

		private sealed class _Runnable_980 : Runnable
		{
			public _Runnable_980(Configuration conf, bool isWebHDFS, Path file3, long length, 
				short numDatanodes, IList<Exception> exceptions)
			{
				this.conf = conf;
				this.isWebHDFS = isWebHDFS;
				this.file3 = file3;
				this.length = length;
				this.numDatanodes = numDatanodes;
				this.exceptions = exceptions;
			}

			public void Run()
			{
				try
				{
					FileSystem fs = TestDFSClientRetries.CreateFsWithDifferentUsername(conf, isWebHDFS
						);
					DFSTestUtil.CreateFile(fs, file3, length, numDatanodes, 20120406L);
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly Configuration conf;

			private readonly bool isWebHDFS;

			private readonly Path file3;

			private readonly long length;

			private readonly short numDatanodes;

			private readonly IList<Exception> exceptions;
		}

		private sealed class _Runnable_995 : Runnable
		{
			public _Runnable_995(URI uri, MiniDFSCluster cluster, IList<Exception> exceptions
				)
			{
				this.uri = uri;
				this.cluster = cluster;
				this.exceptions = exceptions;
			}

			public void Run()
			{
				try
				{
					TimeUnit.Seconds.Sleep(30);
					NUnit.Framework.Assert.IsFalse(HdfsUtils.IsHealthy(uri));
					cluster.RestartNameNode(0, false);
					cluster.WaitActive();
					NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly URI uri;

			private readonly MiniDFSCluster cluster;

			private readonly IList<Exception> exceptions;
		}

		private sealed class _Runnable_1048 : Runnable
		{
			public _Runnable_1048(URI uri, DistributedFileSystem dfs, IList<Exception> exceptions
				)
			{
				this.uri = uri;
				this.dfs = dfs;
				this.exceptions = exceptions;
			}

			public void Run()
			{
				try
				{
					TimeUnit.Seconds.Sleep(30);
					NUnit.Framework.Assert.IsFalse(HdfsUtils.IsHealthy(uri));
					dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
					NUnit.Framework.Assert.IsTrue(HdfsUtils.IsHealthy(uri));
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly URI uri;

			private readonly DistributedFileSystem dfs;

			private readonly IList<Exception> exceptions;
		}

		internal static void AssertEmpty(IList<Exception> exceptions)
		{
			if (!exceptions.IsEmpty())
			{
				StringBuilder b = new StringBuilder("There are ").Append(exceptions.Count).Append
					(" exception(s):");
				for (int i = 0; i < exceptions.Count; i++)
				{
					b.Append("\n  Exception ").Append(i).Append(": ").Append(StringUtils.StringifyException
						(exceptions[i]));
				}
				NUnit.Framework.Assert.Fail(b.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static FileSystem CreateFsWithDifferentUsername(Configuration conf, bool 
			isWebHDFS)
		{
			string username = UserGroupInformation.GetCurrentUser().GetShortUserName() + "_XXX";
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(username, new 
				string[] { "supergroup" });
			return isWebHDFS ? WebHdfsTestUtil.GetWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem
				.Scheme) : DFSTestUtil.GetFileSystemAs(ugi, conf);
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleLinearRandomRetry()
		{
			ParseMultipleLinearRandomRetry(null, string.Empty);
			ParseMultipleLinearRandomRetry(null, "11");
			ParseMultipleLinearRandomRetry(null, "11,22,33");
			ParseMultipleLinearRandomRetry(null, "11,22,33,44,55");
			ParseMultipleLinearRandomRetry(null, "AA");
			ParseMultipleLinearRandomRetry(null, "11,AA");
			ParseMultipleLinearRandomRetry(null, "11,22,33,FF");
			ParseMultipleLinearRandomRetry(null, "11,-22");
			ParseMultipleLinearRandomRetry(null, "-11,22");
			ParseMultipleLinearRandomRetry("[22x11ms]", "11,22");
			ParseMultipleLinearRandomRetry("[22x11ms, 44x33ms]", "11,22,33,44");
			ParseMultipleLinearRandomRetry("[22x11ms, 44x33ms, 66x55ms]", "11,22,33,44,55,66"
				);
			ParseMultipleLinearRandomRetry("[22x11ms, 44x33ms, 66x55ms]", "   11,   22, 33,  44, 55,  66   "
				);
		}

		internal static void ParseMultipleLinearRandomRetry(string expected, string s)
		{
			RetryPolicies.MultipleLinearRandomRetry r = RetryPolicies.MultipleLinearRandomRetry
				.ParseCommaSeparatedString(s);
			Log.Info("input=" + s + ", parsed=" + r + ", expected=" + expected);
			if (r == null)
			{
				NUnit.Framework.Assert.AreEqual(expected, null);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("MultipleLinearRandomRetry" + expected, r.ToString
					());
			}
		}

		/// <summary>
		/// Test that checksum failures are recovered from by the next read on the same
		/// DFSInputStream.
		/// </summary>
		/// <remarks>
		/// Test that checksum failures are recovered from by the next read on the same
		/// DFSInputStream. Corruption information is not persisted from read call to
		/// read call, so the client should expect consecutive calls to behave the same
		/// way. See HDFS-3067.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetryOnChecksumFailure()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				short ReplFactor = 1;
				long FileLength = 512L;
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/corrupted");
				DFSTestUtil.CreateFile(fs, path, FileLength, ReplFactor, 12345L);
				DFSTestUtil.WaitReplication(fs, path, ReplFactor);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, path);
				int blockFilesCorrupted = cluster.CorruptBlockOnDataNodes(block);
				NUnit.Framework.Assert.AreEqual("All replicas not corrupted", ReplFactor, blockFilesCorrupted
					);
				IPEndPoint nnAddr = new IPEndPoint("localhost", cluster.GetNameNodePort());
				DFSClient client = new DFSClient(nnAddr, conf);
				DFSInputStream dis = client.Open(path.ToString());
				byte[] arr = new byte[(int)FileLength];
				for (int i = 0; i < 2; ++i)
				{
					try
					{
						dis.Read(arr, 0, (int)FileLength);
						NUnit.Framework.Assert.Fail("Expected ChecksumException not thrown");
					}
					catch (Exception ex)
					{
						GenericTestUtils.AssertExceptionContains("Checksum error", ex);
					}
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
