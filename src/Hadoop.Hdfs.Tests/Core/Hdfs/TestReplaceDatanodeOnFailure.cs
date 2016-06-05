using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests that a file need not be closed before its
	/// data can be read by another client.
	/// </summary>
	public class TestReplaceDatanodeOnFailure
	{
		internal static readonly Log Log = AppendTestUtil.Log;

		internal static readonly string Dir = "/" + typeof(Org.Apache.Hadoop.Hdfs.TestReplaceDatanodeOnFailure
			).Name + "/";

		internal const short Replication = 3;

		private const string Rack0 = "/rack0";

		private const string Rack1 = "/rack1";

		/// <summary>Test DEFAULT ReplaceDatanodeOnFailure policy.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultPolicy()
		{
			Configuration conf = new HdfsConfiguration();
			ReplaceDatanodeOnFailure p = ReplaceDatanodeOnFailure.Get(conf);
			DatanodeInfo[] infos = new DatanodeInfo[5];
			DatanodeInfo[][] datanodes = new DatanodeInfo[infos.Length + 1][];
			datanodes[0] = new DatanodeInfo[0];
			for (int i = 0; i < infos.Length; )
			{
				infos[i] = DFSTestUtil.GetLocalDatanodeInfo(50020 + i);
				i++;
				datanodes[i] = new DatanodeInfo[i];
				System.Array.Copy(infos, 0, datanodes[i], 0, datanodes[i].Length);
			}
			bool[] isAppend = new bool[] { true, true, false, false };
			bool[] isHflushed = new bool[] { true, false, true, false };
			for (short replication = 1; replication <= infos.Length; replication++)
			{
				for (int nExistings = 0; nExistings < datanodes.Length; nExistings++)
				{
					DatanodeInfo[] existings = datanodes[nExistings];
					NUnit.Framework.Assert.AreEqual(nExistings, existings.Length);
					for (int i_1 = 0; i_1 < isAppend.Length; i_1++)
					{
						for (int j = 0; j < isHflushed.Length; j++)
						{
							int half = replication / 2;
							bool enoughReplica = replication <= nExistings;
							bool noReplica = nExistings == 0;
							bool replicationL3 = replication < 3;
							bool existingsLEhalf = nExistings <= half;
							bool isAH = isAppend[i_1] || isHflushed[j];
							bool expected;
							if (enoughReplica || noReplica || replicationL3)
							{
								expected = false;
							}
							else
							{
								expected = isAH || existingsLEhalf;
							}
							bool computed = p.Satisfy(replication, existings, isAppend[i_1], isHflushed[j]);
							try
							{
								NUnit.Framework.Assert.AreEqual(expected, computed);
							}
							catch (Exception e)
							{
								string s = "replication=" + replication + "\nnExistings =" + nExistings + "\nisAppend   ="
									 + isAppend[i_1] + "\nisHflushed =" + isHflushed[j];
								throw new RuntimeException(s, e);
							}
						}
					}
				}
			}
		}

		/// <summary>Test replace datanode on failure.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceDatanodeOnFailure()
		{
			Configuration conf = new HdfsConfiguration();
			//always replace a datanode
			ReplaceDatanodeOnFailure.Write(ReplaceDatanodeOnFailure.Policy.Always, true, conf
				);
			string[] racks = new string[Replication];
			Arrays.Fill(racks, Rack0);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Racks(racks).NumDataNodes
				(Replication).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path dir = new Path(Dir);
				TestReplaceDatanodeOnFailure.SlowWriter[] slowwriters = new TestReplaceDatanodeOnFailure.SlowWriter
					[10];
				for (int i = 1; i <= slowwriters.Length; i++)
				{
					//create slow writers in different speed
					slowwriters[i - 1] = new TestReplaceDatanodeOnFailure.SlowWriter(fs, new Path(dir
						, "file" + i), i * 200L);
				}
				foreach (TestReplaceDatanodeOnFailure.SlowWriter s in slowwriters)
				{
					s.Start();
				}
				// Let slow writers write something.
				// Some of them are too slow and will be not yet started. 
				SleepSeconds(1);
				//start new datanodes
				cluster.StartDataNodes(conf, 2, true, null, new string[] { Rack1, Rack1 });
				//stop an old datanode
				cluster.StopDataNode(AppendTestUtil.NextInt(Replication));
				//Let the slow writer writes a few more seconds
				//Everyone should have written something.
				SleepSeconds(5);
				//check replication and interrupt.
				foreach (TestReplaceDatanodeOnFailure.SlowWriter s_1 in slowwriters)
				{
					s_1.CheckReplication();
					s_1.InterruptRunning();
				}
				//close files
				foreach (TestReplaceDatanodeOnFailure.SlowWriter s_2 in slowwriters)
				{
					s_2.JoinAndClose();
				}
				//Verify the file
				Log.Info("Verify the file");
				for (int i_1 = 0; i_1 < slowwriters.Length; i_1++)
				{
					Log.Info(slowwriters[i_1].filepath + ": length=" + fs.GetFileStatus(slowwriters[i_1
						].filepath).GetLen());
					FSDataInputStream @in = null;
					try
					{
						@in = fs.Open(slowwriters[i_1].filepath);
						for (int j = 0; (x = @in.Read()) != -1; j++)
						{
							NUnit.Framework.Assert.AreEqual(j, x);
						}
					}
					finally
					{
						IOUtils.CloseStream(@in);
					}
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void SleepSeconds(int waittime)
		{
			Log.Info("Wait " + waittime + " seconds");
			Sharpen.Thread.Sleep(waittime * 1000L);
		}

		internal class SlowWriter : Sharpen.Thread
		{
			internal readonly Path filepath;

			internal readonly HdfsDataOutputStream @out;

			internal readonly long sleepms;

			private volatile bool running = true;

			/// <exception cref="System.IO.IOException"/>
			internal SlowWriter(DistributedFileSystem fs, Path filepath, long sleepms)
				: base(typeof(TestReplaceDatanodeOnFailure.SlowWriter).Name + ":" + filepath)
			{
				this.filepath = filepath;
				this.@out = (HdfsDataOutputStream)fs.Create(filepath, Replication);
				this.sleepms = sleepms;
			}

			public override void Run()
			{
				int i = 0;
				try
				{
					Sleep(sleepms);
					for (; running; i++)
					{
						Log.Info(GetName() + " writes " + i);
						@out.Write(i);
						@out.Hflush();
						Sleep(sleepms);
					}
				}
				catch (Exception e)
				{
					Log.Info(GetName() + " interrupted:" + e);
				}
				catch (IOException e)
				{
					throw new RuntimeException(GetName(), e);
				}
				finally
				{
					Log.Info(GetName() + " terminated: i=" + i);
				}
			}

			internal virtual void InterruptRunning()
			{
				running = false;
				Interrupt();
			}

			/// <exception cref="System.Exception"/>
			internal virtual void JoinAndClose()
			{
				Log.Info(GetName() + " join and close");
				Join();
				IOUtils.CloseStream(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CheckReplication()
			{
				NUnit.Framework.Assert.AreEqual(Replication, @out.GetCurrentBlockReplication());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			Configuration conf = new HdfsConfiguration();
			short Replication = (short)3;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path f = new Path(Dir, "testAppend");
				{
					Log.Info("create an empty file " + f);
					fs.Create(f, Replication).Close();
					FileStatus status = fs.GetFileStatus(f);
					NUnit.Framework.Assert.AreEqual(Replication, status.GetReplication());
					NUnit.Framework.Assert.AreEqual(0L, status.GetLen());
				}
				byte[] bytes = new byte[1000];
				{
					Log.Info("append " + bytes.Length + " bytes to " + f);
					FSDataOutputStream @out = fs.Append(f);
					@out.Write(bytes);
					@out.Close();
					FileStatus status = fs.GetFileStatus(f);
					NUnit.Framework.Assert.AreEqual(Replication, status.GetReplication());
					NUnit.Framework.Assert.AreEqual(bytes.Length, status.GetLen());
				}
				{
					Log.Info("append another " + bytes.Length + " bytes to " + f);
					try
					{
						FSDataOutputStream @out = fs.Append(f);
						@out.Write(bytes);
						@out.Close();
						NUnit.Framework.Assert.Fail();
					}
					catch (IOException ioe)
					{
						Log.Info("This exception is expected", ioe);
					}
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBestEffort()
		{
			Configuration conf = new HdfsConfiguration();
			//always replace a datanode but do not throw exception
			ReplaceDatanodeOnFailure.Write(ReplaceDatanodeOnFailure.Policy.Always, true, conf
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path f = new Path(Dir, "testIgnoreReplaceFailure");
				byte[] bytes = new byte[1000];
				{
					Log.Info("write " + bytes.Length + " bytes to " + f);
					FSDataOutputStream @out = fs.Create(f, Replication);
					@out.Write(bytes);
					@out.Close();
					FileStatus status = fs.GetFileStatus(f);
					NUnit.Framework.Assert.AreEqual(Replication, status.GetReplication());
					NUnit.Framework.Assert.AreEqual(bytes.Length, status.GetLen());
				}
				{
					Log.Info("append another " + bytes.Length + " bytes to " + f);
					FSDataOutputStream @out = fs.Append(f);
					@out.Write(bytes);
					@out.Close();
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		public TestReplaceDatanodeOnFailure()
		{
			{
				((Log4JLogger)DataTransferProtocol.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
