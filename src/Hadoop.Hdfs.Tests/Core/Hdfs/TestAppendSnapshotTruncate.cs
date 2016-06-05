using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test randomly mixing append, snapshot and truncate operations.</summary>
	/// <remarks>
	/// Test randomly mixing append, snapshot and truncate operations.
	/// Use local file system to simulate the each operation and verify
	/// the correctness.
	/// </remarks>
	public class TestAppendSnapshotTruncate
	{
		static TestAppendSnapshotTruncate()
		{
			GenericTestUtils.SetLogLevel(NameNode.stateChangeLog, Level.All);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestAppendSnapshotTruncate
			));

		private const int BlockSize = 1024;

		private const int DatanodeNum = 3;

		private const short Replication = 3;

		private const int FileWorkerNum = 3;

		private const long TestTimeSecond = 10;

		private const long TestTimeoutSecond = TestTimeSecond + 60;

		internal const int ShortHeartbeat = 1;

		internal static readonly string[] EmptyStrings = new string[] {  };

		internal static Configuration conf;

		internal static MiniDFSCluster cluster;

		internal static DistributedFileSystem dfs;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void StartUp()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, ShortHeartbeat);
			conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).Format(true).NumDataNodes(DatanodeNum)
				.NameNodePort(NameNode.DefaultPort).WaitSafeMode(true).Build();
			dfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TearDown()
		{
			if (dfs != null)
			{
				dfs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test randomly mixing append, snapshot and truncate operations.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAST()
		{
			string dirPathString = "/dir";
			Path dir = new Path(dirPathString);
			dfs.Mkdirs(dir);
			dfs.AllowSnapshot(dir);
			FilePath localDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test/data"
				) + dirPathString);
			if (localDir.Exists())
			{
				FileUtil.FullyDelete(localDir);
			}
			localDir.Mkdirs();
			TestAppendSnapshotTruncate.DirWorker w = new TestAppendSnapshotTruncate.DirWorker
				(dir, localDir, FileWorkerNum);
			w.StartAllFiles();
			w.Start();
			TestAppendSnapshotTruncate.Worker.Sleep(TestTimeSecond * 1000);
			w.Stop();
			w.StopAllFiles();
			w.CheckEverything();
		}

		private sealed class _FileFilter_132 : FileFilter
		{
			public _FileFilter_132()
			{
			}

			public bool Accept(FilePath f)
			{
				return f.IsFile();
			}
		}

		internal static readonly FileFilter FileOnly = new _FileFilter_132();

		internal class DirWorker : TestAppendSnapshotTruncate.Worker
		{
			internal readonly Path dir;

			internal readonly FilePath localDir;

			internal readonly TestAppendSnapshotTruncate.FileWorker[] files;

			private IDictionary<string, Path> snapshotPaths = new Dictionary<string, Path>();

			private AtomicInteger snapshotCount = new AtomicInteger();

			/// <exception cref="System.IO.IOException"/>
			internal DirWorker(Path dir, FilePath localDir, int nFiles)
				: base(dir.GetName())
			{
				this.dir = dir;
				this.localDir = localDir;
				this.files = new TestAppendSnapshotTruncate.FileWorker[nFiles];
				for (int i = 0; i < files.Length; i++)
				{
					files[i] = new TestAppendSnapshotTruncate.FileWorker(dir, localDir, string.Format
						("file%02d", i));
				}
			}

			internal static string GetSnapshotName(int n)
			{
				return string.Format("s%02d", n);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string CreateSnapshot(string snapshot)
			{
				StringBuilder b = new StringBuilder("createSnapshot: ").Append(snapshot).Append(" for "
					).Append(dir);
				{
					//copy all local files to a sub dir to simulate snapshot. 
					FilePath subDir = new FilePath(localDir, snapshot);
					NUnit.Framework.Assert.IsFalse(subDir.Exists());
					subDir.Mkdir();
					foreach (FilePath f in localDir.ListFiles(FileOnly))
					{
						FileUtils.CopyFile(f, new FilePath(subDir, f.GetName()));
					}
				}
				Path p = dfs.CreateSnapshot(dir, snapshot);
				snapshotPaths[snapshot] = p;
				return b.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string CheckSnapshot(string snapshot)
			{
				StringBuilder b = new StringBuilder("checkSnapshot: ").Append(snapshot);
				FilePath subDir = new FilePath(localDir, snapshot);
				NUnit.Framework.Assert.IsTrue(subDir.Exists());
				FilePath[] localFiles = subDir.ListFiles(FileOnly);
				Path p = snapshotPaths[snapshot];
				FileStatus[] statuses = dfs.ListStatus(p);
				NUnit.Framework.Assert.AreEqual(localFiles.Length, statuses.Length);
				b.Append(p).Append(" vs ").Append(subDir).Append(", ").Append(statuses.Length).Append
					(" entries");
				Arrays.Sort(localFiles);
				Arrays.Sort(statuses);
				for (int i = 0; i < statuses.Length; i++)
				{
					TestAppendSnapshotTruncate.FileWorker.CheckFullFile(statuses[i].GetPath(), localFiles
						[i]);
				}
				return b.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string DeleteSnapshot(string snapshot)
			{
				StringBuilder b = new StringBuilder("deleteSnapshot: ").Append(snapshot).Append(" from "
					).Append(dir);
				FileUtil.FullyDelete(new FilePath(localDir, snapshot));
				dfs.DeleteSnapshot(dir, snapshot);
				Sharpen.Collections.Remove(snapshotPaths, snapshot);
				return b.ToString();
			}

			/// <exception cref="System.Exception"/>
			public override string Call()
			{
				Random r = DFSUtil.GetRandom();
				int op = r.Next(6);
				if (op <= 1)
				{
					PauseAllFiles();
					try
					{
						string snapshot = GetSnapshotName(snapshotCount.GetAndIncrement());
						return CreateSnapshot(snapshot);
					}
					finally
					{
						StartAllFiles();
					}
				}
				else
				{
					if (op <= 3)
					{
						string[] keys = Sharpen.Collections.ToArray(snapshotPaths.Keys, EmptyStrings);
						if (keys.Length == 0)
						{
							return "NO-OP";
						}
						string snapshot = keys[r.Next(keys.Length)];
						string s = CheckSnapshot(snapshot);
						if (op == 2)
						{
							return DeleteSnapshot(snapshot);
						}
						return s;
					}
					else
					{
						return "NO-OP";
					}
				}
			}

			internal virtual void PauseAllFiles()
			{
				foreach (TestAppendSnapshotTruncate.FileWorker f in files)
				{
					f.Pause();
				}
				for (int i = 0; i < files.Length; )
				{
					Sleep(100);
					for (; i < files.Length && files[i].IsPaused(); i++)
					{
					}
				}
			}

			internal virtual void StartAllFiles()
			{
				foreach (TestAppendSnapshotTruncate.FileWorker f in files)
				{
					f.Start();
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void StopAllFiles()
			{
				foreach (TestAppendSnapshotTruncate.FileWorker f in files)
				{
					f.Stop();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CheckEverything()
			{
				Log.Info("checkEverything");
				foreach (TestAppendSnapshotTruncate.FileWorker f in files)
				{
					f.CheckFullFile();
					f.CheckErrorState();
				}
				foreach (string snapshot in snapshotPaths.Keys)
				{
					CheckSnapshot(snapshot);
				}
				CheckErrorState();
			}
		}

		internal class FileWorker : TestAppendSnapshotTruncate.Worker
		{
			internal readonly Path file;

			internal readonly FilePath localFile;

			/// <exception cref="System.IO.IOException"/>
			internal FileWorker(Path dir, FilePath localDir, string filename)
				: base(filename)
			{
				this.file = new Path(dir, filename);
				this.localFile = new FilePath(localDir, filename);
				localFile.CreateNewFile();
				dfs.Create(file, false, 4096, Replication, BlockSize).Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override string Call()
			{
				Random r = DFSUtil.GetRandom();
				int op = r.Next(9);
				if (op == 0)
				{
					return CheckFullFile();
				}
				else
				{
					int nBlocks = r.Next(4) + 1;
					int lastBlockSize = r.Next(BlockSize) + 1;
					int nBytes = nBlocks * BlockSize + lastBlockSize;
					if (op <= 4)
					{
						return Append(nBytes);
					}
					else
					{
						if (op <= 6)
						{
							return TruncateArbitrarily(nBytes);
						}
						else
						{
							return TruncateToBlockBoundary(nBlocks);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string Append(int n)
			{
				StringBuilder b = new StringBuilder("append ").Append(n).Append(" bytes to ").Append
					(file.GetName());
				byte[] bytes = new byte[n];
				DFSUtil.GetRandom().NextBytes(bytes);
				{
					// write to local file
					FileOutputStream @out = new FileOutputStream(localFile, true);
					@out.Write(bytes, 0, bytes.Length);
					@out.Close();
				}
				{
					FSDataOutputStream @out = dfs.Append(file);
					@out.Write(bytes, 0, bytes.Length);
					@out.Close();
				}
				return b.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string TruncateArbitrarily(int nBytes)
			{
				Preconditions.CheckArgument(nBytes > 0);
				int length = CheckLength();
				StringBuilder b = new StringBuilder("truncateArbitrarily: ").Append(nBytes).Append
					(" bytes from ").Append(file.GetName()).Append(", length=" + length);
				Truncate(length > nBytes ? length - nBytes : 0, b);
				return b.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string TruncateToBlockBoundary(int nBlocks)
			{
				Preconditions.CheckArgument(nBlocks > 0);
				int length = CheckLength();
				StringBuilder b = new StringBuilder("truncateToBlockBoundary: ").Append(nBlocks).
					Append(" blocks from ").Append(file.GetName()).Append(", length=" + length);
				int n = (nBlocks - 1) * BlockSize + (length % BlockSize);
				Preconditions.CheckState(Truncate(length > n ? length - n : 0, b), b);
				return b.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			private bool Truncate(long newLength, StringBuilder b)
			{
				RandomAccessFile raf = new RandomAccessFile(localFile, "rw");
				raf.SetLength(newLength);
				raf.Close();
				bool isReady = dfs.Truncate(file, newLength);
				b.Append(", newLength=").Append(newLength).Append(", isReady=").Append(isReady);
				if (!isReady)
				{
					TestFileTruncate.CheckBlockRecovery(file, dfs, 100, 300L);
				}
				return isReady;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual int CheckLength()
			{
				return CheckLength(file, localFile);
			}

			/// <exception cref="System.IO.IOException"/>
			internal static int CheckLength(Path file, FilePath localFile)
			{
				long length = dfs.GetFileStatus(file).GetLen();
				NUnit.Framework.Assert.AreEqual(localFile.Length(), length);
				NUnit.Framework.Assert.IsTrue(length <= int.MaxValue);
				return (int)length;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string CheckFullFile()
			{
				return CheckFullFile(file, localFile);
			}

			/// <exception cref="System.IO.IOException"/>
			internal static string CheckFullFile(Path file, FilePath localFile)
			{
				StringBuilder b = new StringBuilder("checkFullFile: ").Append(file.GetName()).Append
					(" vs ").Append(localFile);
				byte[] bytes = new byte[CheckLength(file, localFile)];
				b.Append(", length=").Append(bytes.Length);
				FileInputStream @in = new FileInputStream(localFile);
				for (int n = 0; n < bytes.Length; )
				{
					n += @in.Read(bytes, n, bytes.Length - n);
				}
				@in.Close();
				AppendTestUtil.CheckFullFile(dfs, file, bytes.Length, bytes, "File content mismatch: "
					 + b, false);
				return b.ToString();
			}
		}

		internal abstract class Worker : Callable<string>
		{
			[System.Serializable]
			internal sealed class State
			{
				public static readonly TestAppendSnapshotTruncate.Worker.State Idle = new TestAppendSnapshotTruncate.Worker.State
					(false);

				public static readonly TestAppendSnapshotTruncate.Worker.State Running = new TestAppendSnapshotTruncate.Worker.State
					(false);

				public static readonly TestAppendSnapshotTruncate.Worker.State Stopped = new TestAppendSnapshotTruncate.Worker.State
					(true);

				public static readonly TestAppendSnapshotTruncate.Worker.State Error = new TestAppendSnapshotTruncate.Worker.State
					(true);

				internal readonly bool isTerminated;

				internal State(bool isTerminated)
				{
					this.isTerminated = isTerminated;
				}
			}

			internal readonly string name;

			internal readonly AtomicReference<TestAppendSnapshotTruncate.Worker.State> state = 
				new AtomicReference<TestAppendSnapshotTruncate.Worker.State>(TestAppendSnapshotTruncate.Worker.State
				.Idle);

			internal readonly AtomicBoolean isCalling = new AtomicBoolean();

			internal readonly AtomicReference<Sharpen.Thread> thread = new AtomicReference<Sharpen.Thread
				>();

			private Exception thrown = null;

			internal Worker(string name)
			{
				this.name = name;
			}

			internal virtual TestAppendSnapshotTruncate.Worker.State CheckErrorState()
			{
				TestAppendSnapshotTruncate.Worker.State s = state.Get();
				if (s == TestAppendSnapshotTruncate.Worker.State.Error)
				{
					throw new InvalidOperationException(name + " has " + s, thrown);
				}
				return s;
			}

			internal virtual void SetErrorState(Exception t)
			{
				CheckErrorState();
				Log.Error("Worker " + name + " failed.", t);
				state.Set(TestAppendSnapshotTruncate.Worker.State.Error);
				thrown = t;
			}

			internal virtual void Start()
			{
				Preconditions.CheckState(state.CompareAndSet(TestAppendSnapshotTruncate.Worker.State
					.Idle, TestAppendSnapshotTruncate.Worker.State.Running));
				if (thread.Get() == null)
				{
					Sharpen.Thread t = new Sharpen.Thread(null, new _Runnable_446(this), name);
					Preconditions.CheckState(thread.CompareAndSet(null, t));
					t.Start();
				}
			}

			private sealed class _Runnable_446 : Runnable
			{
				public _Runnable_446(Worker _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Run()
				{
					Random r = DFSUtil.GetRandom();
					for (TestAppendSnapshotTruncate.Worker.State s; !(s = this._enclosing.CheckErrorState
						()).isTerminated; )
					{
						if (s == TestAppendSnapshotTruncate.Worker.State.Running)
						{
							this._enclosing.isCalling.Set(true);
							try
							{
								TestAppendSnapshotTruncate.Log.Info(this._enclosing._enclosing._enclosing.Call());
							}
							catch (Exception t)
							{
								this._enclosing.SetErrorState(t);
								return;
							}
							this._enclosing.isCalling.Set(false);
						}
						TestAppendSnapshotTruncate.Worker.Sleep(r.Next(100) + 50);
					}
				}

				private readonly Worker _enclosing;
			}

			internal virtual bool IsPaused()
			{
				TestAppendSnapshotTruncate.Worker.State s = CheckErrorState();
				if (s == TestAppendSnapshotTruncate.Worker.State.Stopped)
				{
					throw new InvalidOperationException(name + " is " + s);
				}
				return s == TestAppendSnapshotTruncate.Worker.State.Idle && !isCalling.Get();
			}

			internal virtual void Pause()
			{
				Preconditions.CheckState(state.CompareAndSet(TestAppendSnapshotTruncate.Worker.State
					.Running, TestAppendSnapshotTruncate.Worker.State.Idle));
			}

			/// <exception cref="System.Exception"/>
			internal virtual void Stop()
			{
				CheckErrorState();
				state.Set(TestAppendSnapshotTruncate.Worker.State.Stopped);
				thread.Get().Join();
			}

			internal static void Sleep(long sleepTimeMs)
			{
				try
				{
					Sharpen.Thread.Sleep(sleepTimeMs);
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			public abstract string Call();
		}
	}
}
