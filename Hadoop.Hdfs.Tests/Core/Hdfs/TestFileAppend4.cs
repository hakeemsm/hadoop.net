using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestFileAppend4
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestFileAppend4));

		internal const long BlockSize = 1024;

		internal const long BbwSize = 500;

		internal static readonly object[] NoArgs = new object[] {  };

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal Path file1;

		internal FSDataOutputStream stm;

		internal readonly bool simulatedStorage = false;

		/* File Append tests for HDFS-200 & HDFS-142, specifically focused on:
		*  using append()/sync() to recover block information
		*/
		// don't align on bytes/checksum
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			this.conf = new Configuration();
			// lower heartbeat interval for fast recognition of DN death
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
			// handle under-replicated blocks quickly (for replication asserts)
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 5);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			// handle failures in the DFSClient pipeline quickly
			// (for cluster.shutdown(); fs.close() idiom)
			conf.SetInt("ipc.client.connect.max.retries", 1);
		}

		/*
		* Recover file.
		* Try and open file in append mode.
		* Doing this, we get a hold of the file that crashed writer
		* was writing to.  Once we have it, close it.  This will
		* allow subsequent reader to see up to last sync.
		* NOTE: This is the same algorithm that HBase uses for file recovery
		* @param fs
		* @throws Exception
		*/
		/// <exception cref="System.Exception"/>
		private void RecoverFile(FileSystem fs)
		{
			Log.Info("Recovering File Lease");
			// set the soft limit to be 1 second so that the
			// namenode triggers lease recovery upon append request
			cluster.SetLeasePeriod(1000, HdfsConstants.LeaseHardlimitPeriod);
			// Trying recovery
			int tries = 60;
			bool recovered = false;
			FSDataOutputStream @out = null;
			while (!recovered && tries-- > 0)
			{
				try
				{
					@out = fs.Append(file1);
					Log.Info("Successfully opened for append");
					recovered = true;
				}
				catch (IOException)
				{
					Log.Info("Failed open for append, waiting on lease recovery");
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
			}
			// ignore it and try again
			if (@out != null)
			{
				@out.Close();
			}
			if (!recovered)
			{
				NUnit.Framework.Assert.Fail("Recovery should take < 1 min");
			}
			Log.Info("Past out lease recovery");
		}

		/// <summary>
		/// Test case that stops a writer after finalizing a block but
		/// before calling completeFile, and then tries to recover
		/// the lease from another thread.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverFinalizedBlock()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(5).Build();
			try
			{
				cluster.WaitActive();
				NamenodeProtocols preSpyNN = cluster.GetNameNodeRpc();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(preSpyNN);
				// Delay completeFile
				GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spyNN).Complete(Matchers.AnyString(), 
					Matchers.AnyString(), (ExtendedBlock)Matchers.AnyObject(), Matchers.AnyLong());
				DFSClient client = new DFSClient(null, spyNN, conf, null);
				file1 = new Path("/testRecoverFinalized");
				OutputStream stm = client.Create("/testRecoverFinalized", true);
				// write 1/2 block
				AppendTestUtil.Write(stm, 0, 4096);
				AtomicReference<Exception> err = new AtomicReference<Exception>();
				Sharpen.Thread t = new _Thread_169(stm, err);
				t.Start();
				Log.Info("Waiting for close to get to latch...");
				delayer.WaitForCall();
				// At this point, the block is finalized on the DNs, but the file
				// has not been completed in the NN.
				// Lose the leases
				Log.Info("Killing lease checker");
				client.GetLeaseRenewer().InterruptAndJoin();
				FileSystem fs1 = cluster.GetFileSystem();
				FileSystem fs2 = AppendTestUtil.CreateHdfsWithDifferentUsername(fs1.GetConf());
				Log.Info("Recovering file");
				RecoverFile(fs2);
				Log.Info("Telling close to proceed.");
				delayer.Proceed();
				Log.Info("Waiting for close to finish.");
				t.Join();
				Log.Info("Close finished.");
				// We expect that close will get a "File is not open"
				// error.
				Exception thrownByClose = err.Get();
				NUnit.Framework.Assert.IsNotNull(thrownByClose);
				NUnit.Framework.Assert.IsTrue(thrownByClose is IOException);
				if (!thrownByClose.Message.Contains("No lease on /testRecoverFinalized"))
				{
					throw thrownByClose;
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Thread_169 : Sharpen.Thread
		{
			public _Thread_169(OutputStream stm, AtomicReference<Exception> err)
			{
				this.stm = stm;
				this.err = err;
			}

			public override void Run()
			{
				try
				{
					stm.Close();
				}
				catch (Exception t)
				{
					err.Set(t);
				}
			}

			private readonly OutputStream stm;

			private readonly AtomicReference<Exception> err;
		}

		/// <summary>
		/// Test case that stops a writer after finalizing a block but
		/// before calling completeFile, recovers a file from another writer,
		/// starts writing from that writer, and then has the old lease holder
		/// call completeFile
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCompleteOtherLeaseHoldersFile()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(5).Build();
			try
			{
				cluster.WaitActive();
				NamenodeProtocols preSpyNN = cluster.GetNameNodeRpc();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(preSpyNN);
				// Delay completeFile
				GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spyNN).Complete(Matchers.AnyString(), 
					Matchers.AnyString(), (ExtendedBlock)Matchers.AnyObject(), Matchers.AnyLong());
				DFSClient client = new DFSClient(null, spyNN, conf, null);
				file1 = new Path("/testCompleteOtherLease");
				OutputStream stm = client.Create("/testCompleteOtherLease", true);
				// write 1/2 block
				AppendTestUtil.Write(stm, 0, 4096);
				AtomicReference<Exception> err = new AtomicReference<Exception>();
				Sharpen.Thread t = new _Thread_242(stm, err);
				t.Start();
				Log.Info("Waiting for close to get to latch...");
				delayer.WaitForCall();
				// At this point, the block is finalized on the DNs, but the file
				// has not been completed in the NN.
				// Lose the leases
				Log.Info("Killing lease checker");
				client.GetLeaseRenewer().InterruptAndJoin();
				FileSystem fs1 = cluster.GetFileSystem();
				FileSystem fs2 = AppendTestUtil.CreateHdfsWithDifferentUsername(fs1.GetConf());
				Log.Info("Recovering file");
				RecoverFile(fs2);
				Log.Info("Opening file for append from new fs");
				FSDataOutputStream appenderStream = fs2.Append(file1);
				Log.Info("Writing some data from new appender");
				AppendTestUtil.Write(appenderStream, 0, 4096);
				Log.Info("Telling old close to proceed.");
				delayer.Proceed();
				Log.Info("Waiting for close to finish.");
				t.Join();
				Log.Info("Close finished.");
				// We expect that close will get a "Lease mismatch"
				// error.
				Exception thrownByClose = err.Get();
				NUnit.Framework.Assert.IsNotNull(thrownByClose);
				NUnit.Framework.Assert.IsTrue(thrownByClose is IOException);
				if (!thrownByClose.Message.Contains("Lease mismatch"))
				{
					throw thrownByClose;
				}
				// The appender should be able to close properly
				appenderStream.Close();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Thread_242 : Sharpen.Thread
		{
			public _Thread_242(OutputStream stm, AtomicReference<Exception> err)
			{
				this.stm = stm;
				this.err = err;
			}

			public override void Run()
			{
				try
				{
					stm.Close();
				}
				catch (Exception t)
				{
					err.Set(t);
				}
			}

			private readonly OutputStream stm;

			private readonly AtomicReference<Exception> err;
		}

		/// <summary>Test the updation of NeededReplications for the Appended Block</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateNeededReplicationsForAppendedFile()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			DistributedFileSystem fileSystem = null;
			try
			{
				// create a file.
				fileSystem = cluster.GetFileSystem();
				Path f = new Path("/testAppend");
				FSDataOutputStream create = fileSystem.Create(f, (short)2);
				create.Write(Sharpen.Runtime.GetBytesForString("/testAppend"));
				create.Close();
				// Append to the file.
				FSDataOutputStream append = fileSystem.Append(f);
				append.Write(Sharpen.Runtime.GetBytesForString("/testAppend"));
				append.Close();
				// Start a new datanode
				cluster.StartDataNodes(conf, 1, true, null, null);
				// Check for replications
				DFSTestUtil.WaitReplication(fileSystem, f, (short)2);
			}
			finally
			{
				if (null != fileSystem)
				{
					fileSystem.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that an append with no locations fails with an exception
		/// showing insufficient locations.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppendInsufficientLocations()
		{
			Configuration conf = new Configuration();
			// lower heartbeat interval for fast recognition of DN
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 3000);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			DistributedFileSystem fileSystem = null;
			try
			{
				// create a file with replication 3
				fileSystem = cluster.GetFileSystem();
				Path f = new Path("/testAppend");
				FSDataOutputStream create = fileSystem.Create(f, (short)2);
				create.Write(Sharpen.Runtime.GetBytesForString("/testAppend"));
				create.Close();
				// Check for replications
				DFSTestUtil.WaitReplication(fileSystem, f, (short)2);
				// Shut down all DNs that have the last block location for the file
				LocatedBlocks lbs = fileSystem.dfs.GetNamenode().GetBlockLocations("/testAppend", 
					0, long.MaxValue);
				IList<DataNode> dnsOfCluster = cluster.GetDataNodes();
				DatanodeInfo[] dnsWithLocations = lbs.GetLastLocatedBlock().GetLocations();
				foreach (DataNode dn in dnsOfCluster)
				{
					foreach (DatanodeInfo loc in dnsWithLocations)
					{
						if (dn.GetDatanodeId().Equals(loc))
						{
							dn.Shutdown();
							DFSTestUtil.WaitForDatanodeDeath(dn);
						}
					}
				}
				// Wait till 0 replication is recognized
				DFSTestUtil.WaitReplication(fileSystem, f, (short)0);
				// Append to the file, at this state there are 3 live DNs but none of them
				// have the block.
				try
				{
					fileSystem.Append(f);
					NUnit.Framework.Assert.Fail("Append should fail because insufficient locations");
				}
				catch (IOException e)
				{
					Log.Info("Expected exception: ", e);
				}
				FSDirectory dir = cluster.GetNamesystem().GetFSDirectory();
				INodeFile inode = INodeFile.ValueOf(dir.GetINode("/testAppend"), "/testAppend");
				NUnit.Framework.Assert.IsTrue("File should remain closed", !inode.IsUnderConstruction
					());
			}
			finally
			{
				if (null != fileSystem)
				{
					fileSystem.Close();
				}
				cluster.Shutdown();
			}
		}

		public TestFileAppend4()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
				GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
				GenericTestUtils.SetLogLevel(DFSClient.Log, Level.All);
			}
		}
	}
}
