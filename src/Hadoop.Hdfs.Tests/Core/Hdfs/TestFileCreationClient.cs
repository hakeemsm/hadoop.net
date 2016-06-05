using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests that a file need not be closed before its
	/// data can be read by another client.
	/// </summary>
	public class TestFileCreationClient
	{
		internal static readonly string Dir = "/" + typeof(TestFileCreationClient).Name +
			 "/";

		/// <summary>Test lease recovery Triggered by DFSClient.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClientTriggeredLeaseRecovery()
		{
			int Replication = 3;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 1);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, Replication);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path dir = new Path("/wrwelkj");
				TestFileCreationClient.SlowWriter[] slowwriters = new TestFileCreationClient.SlowWriter
					[10];
				for (int i = 0; i < slowwriters.Length; i++)
				{
					slowwriters[i] = new TestFileCreationClient.SlowWriter(fs, new Path(dir, "file" +
						 i));
				}
				try
				{
					for (int i_1 = 0; i_1 < slowwriters.Length; i_1++)
					{
						slowwriters[i_1].Start();
					}
					Sharpen.Thread.Sleep(1000);
					// let writers get started
					//stop a datanode, it should have least recover.
					cluster.StopDataNode(AppendTestUtil.NextInt(Replication));
					//let the slow writer writes a few more seconds
					System.Console.Out.WriteLine("Wait a few seconds");
					Sharpen.Thread.Sleep(5000);
				}
				finally
				{
					for (int i_1 = 0; i_1 < slowwriters.Length; i_1++)
					{
						if (slowwriters[i_1] != null)
						{
							slowwriters[i_1].running = false;
							slowwriters[i_1].Interrupt();
						}
					}
					for (int i_2 = 0; i_2 < slowwriters.Length; i_2++)
					{
						if (slowwriters[i_2] != null)
						{
							slowwriters[i_2].Join();
						}
					}
				}
				//Verify the file
				System.Console.Out.WriteLine("Verify the file");
				for (int i_3 = 0; i_3 < slowwriters.Length; i_3++)
				{
					System.Console.Out.WriteLine(slowwriters[i_3].filepath + ": length=" + fs.GetFileStatus
						(slowwriters[i_3].filepath).GetLen());
					FSDataInputStream @in = null;
					try
					{
						@in = fs.Open(slowwriters[i_3].filepath);
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

		internal class SlowWriter : Sharpen.Thread
		{
			internal readonly FileSystem fs;

			internal readonly Path filepath;

			internal bool running = true;

			internal SlowWriter(FileSystem fs, Path filepath)
				: base(typeof(TestFileCreationClient.SlowWriter).Name + ":" + filepath)
			{
				this.fs = fs;
				this.filepath = filepath;
			}

			public override void Run()
			{
				FSDataOutputStream @out = null;
				int i = 0;
				try
				{
					@out = fs.Create(filepath);
					for (; running; i++)
					{
						System.Console.Out.WriteLine(GetName() + " writes " + i);
						@out.Write(i);
						@out.Hflush();
						Sleep(100);
					}
				}
				catch (Exception e)
				{
					System.Console.Out.WriteLine(GetName() + " dies: e=" + e);
				}
				finally
				{
					System.Console.Out.WriteLine(GetName() + ": i=" + i);
					IOUtils.CloseStream(@out);
				}
			}
		}

		public TestFileCreationClient()
		{
			{
				((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LeaseManager.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
				((Log4JLogger)InterDatanodeProtocol.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
