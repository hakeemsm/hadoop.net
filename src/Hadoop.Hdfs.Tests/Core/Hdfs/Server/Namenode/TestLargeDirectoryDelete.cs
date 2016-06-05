using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Ensure during large directory delete, namenode does not block until the
	/// deletion completes and handles new requests from other clients
	/// </summary>
	public class TestLargeDirectoryDelete
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLargeDirectoryDelete
			));

		private static readonly Configuration Conf = new HdfsConfiguration();

		private const int TotalBlocks = 10000;

		private MiniDFSCluster mc = null;

		private int createOps = 0;

		private int lockOps = 0;

		static TestLargeDirectoryDelete()
		{
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 1);
		}

		/// <summary>create a file with a length of <code>filelen</code></summary>
		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(string fileName, long filelen)
		{
			FileSystem fs = mc.GetFileSystem();
			Path filePath = new Path(fileName);
			DFSTestUtil.CreateFile(fs, filePath, filelen, (short)1, 0);
		}

		/// <summary>Create a large number of directories and files</summary>
		/// <exception cref="System.IO.IOException"/>
		private void CreateFiles()
		{
			Random rand = new Random();
			// Create files in a directory with random depth
			// ranging from 0-10.
			for (int i = 0; i < TotalBlocks; i += 100)
			{
				string filename = "/root/";
				int dirs = rand.Next(10);
				// Depth of the directory
				for (int j = i; j >= (i - dirs); j--)
				{
					filename += j + "/";
				}
				filename += "file" + i;
				CreateFile(filename, 100);
			}
		}

		private int GetBlockCount()
		{
			NUnit.Framework.Assert.IsNotNull("Null cluster", mc);
			NUnit.Framework.Assert.IsNotNull("No Namenode in cluster", mc.GetNameNode());
			FSNamesystem namesystem = mc.GetNamesystem();
			NUnit.Framework.Assert.IsNotNull("Null Namesystem in cluster", namesystem);
			NUnit.Framework.Assert.IsNotNull("Null Namesystem.blockmanager", namesystem.GetBlockManager
				());
			return (int)namesystem.GetBlocksTotal();
		}

		/// <summary>
		/// Run multiple threads doing simultaneous operations on the namenode
		/// while a large directory is being deleted.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void RunThreads()
		{
			TestLargeDirectoryDelete.TestThread[] threads = new TestLargeDirectoryDelete.TestThread
				[2];
			// Thread for creating files
			threads[0] = new _TestThread_93(this);
			// Thread that periodically acquires the FSNamesystem lock
			threads[1] = new _TestThread_114(this);
			threads[0].Start();
			threads[1].Start();
			long start = Time.Now();
			FSNamesystem.BlockDeletionIncrement = 1;
			mc.GetFileSystem().Delete(new Path("/root"), true);
			// recursive delete
			long end = Time.Now();
			threads[0].EndThread();
			threads[1].EndThread();
			Log.Info("Deletion took " + (end - start) + "msecs");
			Log.Info("createOperations " + createOps);
			Log.Info("lockOperations " + lockOps);
			NUnit.Framework.Assert.IsTrue(lockOps + createOps > 0);
			threads[0].Rethrow();
			threads[1].Rethrow();
		}

		private sealed class _TestThread_93 : TestLargeDirectoryDelete.TestThread
		{
			public _TestThread_93(TestLargeDirectoryDelete _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected internal override void Execute()
			{
				while (this.live)
				{
					try
					{
						int blockcount = this._enclosing.GetBlockCount();
						if (blockcount < TestLargeDirectoryDelete.TotalBlocks && blockcount > 0)
						{
							string file = "/tmp" + this._enclosing.createOps;
							this._enclosing.CreateFile(file, 1);
							this._enclosing.mc.GetFileSystem().Delete(new Path(file), true);
							this._enclosing.createOps++;
						}
					}
					catch (IOException ex)
					{
						TestLargeDirectoryDelete.Log.Info("createFile exception ", ex);
						break;
					}
				}
			}

			private readonly TestLargeDirectoryDelete _enclosing;
		}

		private sealed class _TestThread_114 : TestLargeDirectoryDelete.TestThread
		{
			public _TestThread_114(TestLargeDirectoryDelete _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected internal override void Execute()
			{
				while (this.live)
				{
					try
					{
						int blockcount = this._enclosing.GetBlockCount();
						if (blockcount < TestLargeDirectoryDelete.TotalBlocks && blockcount > 0)
						{
							this._enclosing.mc.GetNamesystem().WriteLock();
							try
							{
								this._enclosing.lockOps++;
							}
							finally
							{
								this._enclosing.mc.GetNamesystem().WriteUnlock();
							}
							Sharpen.Thread.Sleep(1);
						}
					}
					catch (Exception ex)
					{
						TestLargeDirectoryDelete.Log.Info("lockOperation exception ", ex);
						break;
					}
				}
			}

			private readonly TestLargeDirectoryDelete _enclosing;
		}

		/// <summary>
		/// An abstract class for tests that catches exceptions and can
		/// rethrow them on a different thread, and has an
		/// <see cref="EndThread()"/>
		/// 
		/// operation that flips a volatile boolean before interrupting the thread.
		/// Also: after running the implementation of
		/// <see cref="Execute()"/>
		/// in the
		/// implementation class, the thread is notified: other threads can wait
		/// for it to terminate
		/// </summary>
		private abstract class TestThread : Sharpen.Thread
		{
			internal volatile Exception thrown;

			protected internal volatile bool live = true;

			public override void Run()
			{
				try
				{
					this.Execute();
				}
				catch (Exception throwable)
				{
					TestLargeDirectoryDelete.Log.Warn(throwable);
					this.SetThrown(throwable);
				}
				finally
				{
					lock (this)
					{
						Sharpen.Runtime.Notify(this);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			protected internal abstract void Execute();

			protected internal virtual void SetThrown(Exception thrown)
			{
				lock (this)
				{
					this.thrown = thrown;
				}
			}

			/// <summary>Rethrow anything caught</summary>
			/// <exception cref="System.Exception">any non-null throwable raised by the execute method.
			/// 	</exception>
			public virtual void Rethrow()
			{
				lock (this)
				{
					if (this.thrown != null)
					{
						throw this.thrown;
					}
				}
			}

			/// <summary>End the thread by setting the live p</summary>
			public virtual void EndThread()
			{
				lock (this)
				{
					this.live = false;
					this.Interrupt();
					try
					{
						Sharpen.Runtime.Wait(this);
					}
					catch (Exception e)
					{
						if (TestLargeDirectoryDelete.Log.IsDebugEnabled())
						{
							TestLargeDirectoryDelete.Log.Debug("Ignoring " + e, e);
						}
					}
				}
			}

			internal TestThread(TestLargeDirectoryDelete _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestLargeDirectoryDelete _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void LargeDelete()
		{
			mc = new MiniDFSCluster.Builder(Conf).Build();
			try
			{
				mc.WaitActive();
				NUnit.Framework.Assert.IsNotNull("No Namenode in cluster", mc.GetNameNode());
				CreateFiles();
				NUnit.Framework.Assert.AreEqual(TotalBlocks, GetBlockCount());
				RunThreads();
			}
			finally
			{
				mc.Shutdown();
			}
		}
	}
}
