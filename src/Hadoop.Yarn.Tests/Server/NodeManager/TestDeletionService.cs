using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestDeletionService
	{
		private static readonly FileContext lfs = GetLfs();

		private static FileContext GetLfs()
		{
			try
			{
				return FileContext.GetLocalFSFileContext();
			}
			catch (UnsupportedFileSystemException e)
			{
				throw new RuntimeException(e);
			}
		}

		private static readonly Path @base = lfs.MakeQualified(new Path("target", typeof(
			TestDeletionService).FullName));

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void RemoveBase()
		{
			lfs.Delete(@base, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<Path> BuildDirs(Random r, Path root, int numpaths)
		{
			AList<Path> ret = new AList<Path>();
			for (int i = 0; i < numpaths; ++i)
			{
				Path p = root;
				long name = r.NextLong();
				do
				{
					p = new Path(p, string.Empty + name);
					name = r.NextLong();
				}
				while (0 == (name % 2));
				ret.AddItem(p);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateDirs(Path @base, IList<Path> dirs)
		{
			foreach (Path dir in dirs)
			{
				lfs.Mkdir(new Path(@base, dir), null, true);
			}
		}

		internal class FakeDefaultContainerExecutor : DefaultContainerExecutor
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void DeleteAsUser(string user, Path subDir, params Path[] basedirs
				)
			{
				if ((long.Parse(subDir.GetName()) % 2) == 0)
				{
					NUnit.Framework.Assert.IsNull(user);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("dingo", user);
				}
				base.DeleteAsUser(user, subDir, basedirs);
				NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(subDir));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAbsDelete()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			IList<Path> dirs = BuildDirs(r, @base, 20);
			CreateDirs(new Path("."), dirs);
			TestDeletionService.FakeDefaultContainerExecutor exec = new TestDeletionService.FakeDefaultContainerExecutor
				();
			Configuration conf = new Configuration();
			exec.SetConf(conf);
			DeletionService del = new DeletionService(exec);
			del.Init(conf);
			del.Start();
			try
			{
				foreach (Path p in dirs)
				{
					del.Delete((long.Parse(p.GetName()) % 2) == 0 ? null : "dingo", p, null);
				}
				int msecToWait = 20 * 1000;
				foreach (Path p_1 in dirs)
				{
					while (msecToWait > 0 && lfs.Util().Exists(p_1))
					{
						Sharpen.Thread.Sleep(100);
						msecToWait -= 100;
					}
					NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(p_1));
				}
			}
			finally
			{
				del.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelativeDelete()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			IList<Path> baseDirs = BuildDirs(r, @base, 4);
			CreateDirs(new Path("."), baseDirs);
			IList<Path> content = BuildDirs(r, new Path("."), 10);
			foreach (Path b in baseDirs)
			{
				CreateDirs(b, content);
			}
			DeletionService del = new DeletionService(new TestDeletionService.FakeDefaultContainerExecutor
				());
			try
			{
				del.Init(new Configuration());
				del.Start();
				foreach (Path p in content)
				{
					NUnit.Framework.Assert.IsTrue(lfs.Util().Exists(new Path(baseDirs[0], p)));
					del.Delete((long.Parse(p.GetName()) % 2) == 0 ? null : "dingo", p, Sharpen.Collections.ToArray
						(baseDirs, new Path[4]));
				}
				int msecToWait = 20 * 1000;
				foreach (Path p_1 in baseDirs)
				{
					foreach (Path q in content)
					{
						Path fp = new Path(p_1, q);
						while (msecToWait > 0 && lfs.Util().Exists(fp))
						{
							Sharpen.Thread.Sleep(100);
							msecToWait -= 100;
						}
						NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(fp));
					}
				}
			}
			finally
			{
				del.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoDelete()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			IList<Path> dirs = BuildDirs(r, @base, 20);
			CreateDirs(new Path("."), dirs);
			TestDeletionService.FakeDefaultContainerExecutor exec = new TestDeletionService.FakeDefaultContainerExecutor
				();
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.DebugNmDeleteDelaySec, -1);
			exec.SetConf(conf);
			DeletionService del = new DeletionService(exec);
			try
			{
				del.Init(conf);
				del.Start();
				foreach (Path p in dirs)
				{
					del.Delete((long.Parse(p.GetName()) % 2) == 0 ? null : "dingo", p, null);
				}
				int msecToWait = 20 * 1000;
				foreach (Path p_1 in dirs)
				{
					while (msecToWait > 0 && lfs.Util().Exists(p_1))
					{
						Sharpen.Thread.Sleep(100);
						msecToWait -= 100;
					}
					NUnit.Framework.Assert.IsTrue(lfs.Util().Exists(p_1));
				}
			}
			finally
			{
				del.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopWithDelayedTasks()
		{
			DeletionService del = new DeletionService(Org.Mockito.Mockito.Mock<ContainerExecutor
				>());
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.DebugNmDeleteDelaySec, 60);
			try
			{
				del.Init(conf);
				del.Start();
				del.Delete("dingo", new Path("/does/not/exist"));
			}
			finally
			{
				del.Stop();
			}
			NUnit.Framework.Assert.IsTrue(del.IsTerminated());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFileDeletionTaskDependency()
		{
			TestDeletionService.FakeDefaultContainerExecutor exec = new TestDeletionService.FakeDefaultContainerExecutor
				();
			Configuration conf = new Configuration();
			exec.SetConf(conf);
			DeletionService del = new DeletionService(exec);
			del.Init(conf);
			del.Start();
			try
			{
				Random r = new Random();
				long seed = r.NextLong();
				r.SetSeed(seed);
				System.Console.Out.WriteLine("SEED: " + seed);
				IList<Path> dirs = BuildDirs(r, @base, 2);
				CreateDirs(new Path("."), dirs);
				// first we will try to delete sub directories which are present. This
				// should then trigger parent directory to be deleted.
				IList<Path> subDirs = BuildDirs(r, dirs[0], 2);
				DeletionService.FileDeletionTask dependentDeletionTask = del.CreateFileDeletionTask
					(null, dirs[0], new Path[] {  });
				IList<DeletionService.FileDeletionTask> deletionTasks = new AList<DeletionService.FileDeletionTask
					>();
				foreach (Path subDir in subDirs)
				{
					DeletionService.FileDeletionTask deletionTask = del.CreateFileDeletionTask(null, 
						null, new Path[] { subDir });
					deletionTask.AddFileDeletionTaskDependency(dependentDeletionTask);
					deletionTasks.AddItem(deletionTask);
				}
				foreach (DeletionService.FileDeletionTask task in deletionTasks)
				{
					del.ScheduleFileDeletionTask(task);
				}
				int msecToWait = 20 * 1000;
				while (msecToWait > 0 && (lfs.Util().Exists(dirs[0])))
				{
					Sharpen.Thread.Sleep(100);
					msecToWait -= 100;
				}
				NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(dirs[0]));
				// Now we will try to delete sub directories; one of the deletion task we
				// will mark as failure and then parent directory should not be deleted.
				subDirs = BuildDirs(r, dirs[1], 2);
				subDirs.AddItem(new Path(dirs[1], "absentFile"));
				dependentDeletionTask = del.CreateFileDeletionTask(null, dirs[1], new Path[] {  }
					);
				deletionTasks = new AList<DeletionService.FileDeletionTask>();
				foreach (Path subDir_1 in subDirs)
				{
					DeletionService.FileDeletionTask deletionTask = del.CreateFileDeletionTask(null, 
						null, new Path[] { subDir_1 });
					deletionTask.AddFileDeletionTaskDependency(dependentDeletionTask);
					deletionTasks.AddItem(deletionTask);
				}
				// marking one of the tasks as a failure.
				deletionTasks[2].SetSuccess(false);
				foreach (DeletionService.FileDeletionTask task_1 in deletionTasks)
				{
					del.ScheduleFileDeletionTask(task_1);
				}
				msecToWait = 20 * 1000;
				while (msecToWait > 0 && (lfs.Util().Exists(subDirs[0]) || lfs.Util().Exists(subDirs
					[1])))
				{
					Sharpen.Thread.Sleep(100);
					msecToWait -= 100;
				}
				NUnit.Framework.Assert.IsTrue(lfs.Util().Exists(dirs[1]));
			}
			finally
			{
				del.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("SEED: " + seed);
			IList<Path> baseDirs = BuildDirs(r, @base, 4);
			CreateDirs(new Path("."), baseDirs);
			IList<Path> content = BuildDirs(r, new Path("."), 10);
			foreach (Path b in baseDirs)
			{
				CreateDirs(b, content);
			}
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			conf.SetInt(YarnConfiguration.DebugNmDeleteDelaySec, 1);
			NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			DeletionService del = new DeletionService(new TestDeletionService.FakeDefaultContainerExecutor
				(), stateStore);
			try
			{
				del.Init(conf);
				del.Start();
				foreach (Path p in content)
				{
					NUnit.Framework.Assert.IsTrue(lfs.Util().Exists(new Path(baseDirs[0], p)));
					del.Delete((long.Parse(p.GetName()) % 2) == 0 ? null : "dingo", p, Sharpen.Collections.ToArray
						(baseDirs, new Path[4]));
				}
				// restart the deletion service
				del.Stop();
				del = new DeletionService(new TestDeletionService.FakeDefaultContainerExecutor(), 
					stateStore);
				del.Init(conf);
				del.Start();
				// verify paths are still eventually deleted
				int msecToWait = 10 * 1000;
				foreach (Path p_1 in baseDirs)
				{
					foreach (Path q in content)
					{
						Path fp = new Path(p_1, q);
						while (msecToWait > 0 && lfs.Util().Exists(fp))
						{
							Sharpen.Thread.Sleep(100);
							msecToWait -= 100;
						}
						NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(fp));
					}
				}
			}
			finally
			{
				del.Close();
				stateStore.Close();
			}
		}
	}
}
