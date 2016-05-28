using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Server.Tasktracker;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestIndexCache : TestCase
	{
		private JobConf conf;

		private FileSystem fs;

		private Path p;

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new JobConf();
			fs = FileSystem.GetLocal(conf).GetRaw();
			p = new Path(Runtime.GetProperty("test.build.data", "/tmp"), "cache").MakeQualified
				(fs.GetUri(), fs.GetWorkingDirectory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLRCPolicy()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("seed: " + seed);
			fs.Delete(p, true);
			conf.SetInt(TTConfig.TtIndexCache, 1);
			int partsPerMap = 1000;
			int bytesPerFile = partsPerMap * 24;
			IndexCache cache = new IndexCache(conf);
			// fill cache
			int totalsize = bytesPerFile;
			for (; totalsize < 1024 * 1024; totalsize += bytesPerFile)
			{
				Path f = new Path(p, Sharpen.Extensions.ToString(totalsize, 36));
				WriteFile(fs, f, totalsize, partsPerMap);
				IndexRecord rec = cache.GetIndexInformation(Sharpen.Extensions.ToString(totalsize
					, 36), r.Next(partsPerMap), f, UserGroupInformation.GetCurrentUser().GetShortUserName
					());
				CheckRecord(rec, totalsize);
			}
			// delete files, ensure cache retains all elem
			foreach (FileStatus stat in fs.ListStatus(p))
			{
				fs.Delete(stat.GetPath(), true);
			}
			for (int i = bytesPerFile; i < 1024 * 1024; i += bytesPerFile)
			{
				Path f = new Path(p, Sharpen.Extensions.ToString(i, 36));
				IndexRecord rec = cache.GetIndexInformation(Sharpen.Extensions.ToString(i, 36), r
					.Next(partsPerMap), f, UserGroupInformation.GetCurrentUser().GetShortUserName());
				CheckRecord(rec, i);
			}
			// push oldest (bytesPerFile) out of cache
			Path f_1 = new Path(p, Sharpen.Extensions.ToString(totalsize, 36));
			WriteFile(fs, f_1, totalsize, partsPerMap);
			cache.GetIndexInformation(Sharpen.Extensions.ToString(totalsize, 36), r.Next(partsPerMap
				), f_1, UserGroupInformation.GetCurrentUser().GetShortUserName());
			fs.Delete(f_1, false);
			// oldest fails to read, or error
			bool fnf = false;
			try
			{
				cache.GetIndexInformation(Sharpen.Extensions.ToString(bytesPerFile, 36), r.Next(partsPerMap
					), new Path(p, Sharpen.Extensions.ToString(bytesPerFile)), UserGroupInformation.
					GetCurrentUser().GetShortUserName());
			}
			catch (IOException e)
			{
				if (e.InnerException == null || !(e.InnerException is FileNotFoundException))
				{
					throw;
				}
				else
				{
					fnf = true;
				}
			}
			if (!fnf)
			{
				Fail("Failed to push out last entry");
			}
			// should find all the other entries
			for (int i_1 = bytesPerFile << 1; i_1 < 1024 * 1024; i_1 += bytesPerFile)
			{
				IndexRecord rec = cache.GetIndexInformation(Sharpen.Extensions.ToString(i_1, 36), 
					r.Next(partsPerMap), new Path(p, Sharpen.Extensions.ToString(i_1, 36)), UserGroupInformation
					.GetCurrentUser().GetShortUserName());
				CheckRecord(rec, i_1);
			}
			IndexRecord rec_1 = cache.GetIndexInformation(Sharpen.Extensions.ToString(totalsize
				, 36), r.Next(partsPerMap), f_1, UserGroupInformation.GetCurrentUser().GetShortUserName
				());
			CheckRecord(rec_1, totalsize);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBadIndex()
		{
			int parts = 30;
			fs.Delete(p, true);
			conf.SetInt(TTConfig.TtIndexCache, 1);
			IndexCache cache = new IndexCache(conf);
			Path f = new Path(p, "badindex");
			FSDataOutputStream @out = fs.Create(f, false);
			CheckedOutputStream iout = new CheckedOutputStream(@out, new CRC32());
			DataOutputStream dout = new DataOutputStream(iout);
			for (int i = 0; i < parts; ++i)
			{
				for (int j = 0; j < MapTask.MapOutputIndexRecordLength / 8; ++j)
				{
					if (0 == (i % 3))
					{
						dout.WriteLong(i);
					}
					else
					{
						@out.WriteLong(i);
					}
				}
			}
			@out.WriteLong(iout.GetChecksum().GetValue());
			dout.Close();
			try
			{
				cache.GetIndexInformation("badindex", 7, f, UserGroupInformation.GetCurrentUser()
					.GetShortUserName());
				Fail("Did not detect bad checksum");
			}
			catch (IOException e)
			{
				if (!(e.InnerException is ChecksumException))
				{
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidReduceNumberOrLength()
		{
			fs.Delete(p, true);
			conf.SetInt(TTConfig.TtIndexCache, 1);
			int partsPerMap = 1000;
			int bytesPerFile = partsPerMap * 24;
			IndexCache cache = new IndexCache(conf);
			// fill cache
			Path feq = new Path(p, "invalidReduceOrPartsPerMap");
			WriteFile(fs, feq, bytesPerFile, partsPerMap);
			// Number of reducers should always be less than partsPerMap as reducer
			// numbers start from 0 and there cannot be more reducer than parts
			try
			{
				// Number of reducers equal to partsPerMap
				cache.GetIndexInformation("reduceEqualPartsPerMap", partsPerMap, feq, UserGroupInformation
					.GetCurrentUser().GetShortUserName());
				// reduce number == partsPerMap
				Fail("Number of reducers equal to partsPerMap did not fail");
			}
			catch (Exception e)
			{
				if (!(e is IOException))
				{
					throw;
				}
			}
			try
			{
				// Number of reducers more than partsPerMap
				cache.GetIndexInformation("reduceMorePartsPerMap", partsPerMap + 1, feq, UserGroupInformation
					.GetCurrentUser().GetShortUserName());
				// reduce number > partsPerMap
				Fail("Number of reducers more than partsPerMap did not fail");
			}
			catch (Exception e)
			{
				if (!(e is IOException))
				{
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveMap()
		{
			// This test case use two thread to call getIndexInformation and 
			// removeMap concurrently, in order to construct race condition.
			// This test case may not repeatable. But on my macbook this test 
			// fails with probability of 100% on code before MAPREDUCE-2541,
			// so it is repeatable in practice.
			fs.Delete(p, true);
			conf.SetInt(TTConfig.TtIndexCache, 10);
			// Make a big file so removeMapThread almost surely runs faster than 
			// getInfoThread 
			int partsPerMap = 100000;
			int bytesPerFile = partsPerMap * 24;
			IndexCache cache = new IndexCache(conf);
			Path big = new Path(p, "bigIndex");
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			WriteFile(fs, big, bytesPerFile, partsPerMap);
			// run multiple times
			for (int i = 0; i < 20; ++i)
			{
				Sharpen.Thread getInfoThread = new _Thread_216(cache, partsPerMap, big, user);
				// should not be here
				Sharpen.Thread removeMapThread = new _Thread_226(cache);
				if (i % 2 == 0)
				{
					getInfoThread.Start();
					removeMapThread.Start();
				}
				else
				{
					removeMapThread.Start();
					getInfoThread.Start();
				}
				getInfoThread.Join();
				removeMapThread.Join();
				NUnit.Framework.Assert.AreEqual(true, cache.CheckTotalMemoryUsed());
			}
		}

		private sealed class _Thread_216 : Sharpen.Thread
		{
			public _Thread_216(IndexCache cache, int partsPerMap, Path big, string user)
			{
				this.cache = cache;
				this.partsPerMap = partsPerMap;
				this.big = big;
				this.user = user;
			}

			public override void Run()
			{
				try
				{
					cache.GetIndexInformation("bigIndex", partsPerMap, big, user);
				}
				catch (Exception)
				{
				}
			}

			private readonly IndexCache cache;

			private readonly int partsPerMap;

			private readonly Path big;

			private readonly string user;
		}

		private sealed class _Thread_226 : Sharpen.Thread
		{
			public _Thread_226(IndexCache cache)
			{
				this.cache = cache;
			}

			public override void Run()
			{
				cache.RemoveMap("bigIndex");
			}

			private readonly IndexCache cache;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateRace()
		{
			fs.Delete(p, true);
			conf.SetInt(TTConfig.TtIndexCache, 1);
			int partsPerMap = 1000;
			int bytesPerFile = partsPerMap * 24;
			IndexCache cache = new IndexCache(conf);
			Path racy = new Path(p, "racyIndex");
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			WriteFile(fs, racy, bytesPerFile, partsPerMap);
			// run multiple instances
			Sharpen.Thread[] getInfoThreads = new Sharpen.Thread[50];
			for (int i = 0; i < 50; i++)
			{
				getInfoThreads[i] = new _Thread_260(cache, partsPerMap, racy, user);
			}
			// should not be here
			for (int i_1 = 0; i_1 < 50; i_1++)
			{
				getInfoThreads[i_1].Start();
			}
			Sharpen.Thread mainTestThread = Sharpen.Thread.CurrentThread();
			Sharpen.Thread timeoutThread = new _Thread_279(mainTestThread);
			// we are done;
			for (int i_2 = 0; i_2 < 50; i_2++)
			{
				try
				{
					getInfoThreads[i_2].Join();
				}
				catch (Exception)
				{
					// we haven't finished in time. Potential deadlock/race.
					Fail("Unexpectedly long delay during concurrent cache entry creations");
				}
			}
			// stop the timeoutThread. If we get interrupted before stopping, there
			// must be something wrong, although it wasn't a deadlock. No need to
			// catch and swallow.
			timeoutThread.Interrupt();
		}

		private sealed class _Thread_260 : Sharpen.Thread
		{
			public _Thread_260(IndexCache cache, int partsPerMap, Path racy, string user)
			{
				this.cache = cache;
				this.partsPerMap = partsPerMap;
				this.racy = racy;
				this.user = user;
			}

			public override void Run()
			{
				try
				{
					cache.GetIndexInformation("racyIndex", partsPerMap, racy, user);
					cache.RemoveMap("racyIndex");
				}
				catch (Exception)
				{
				}
			}

			private readonly IndexCache cache;

			private readonly int partsPerMap;

			private readonly Path racy;

			private readonly string user;
		}

		private sealed class _Thread_279 : Sharpen.Thread
		{
			public _Thread_279(Sharpen.Thread mainTestThread)
			{
				this.mainTestThread = mainTestThread;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(15000);
					mainTestThread.Interrupt();
				}
				catch (Exception)
				{
				}
			}

			private readonly Sharpen.Thread mainTestThread;
		}

		private static void CheckRecord(IndexRecord rec, long fill)
		{
			NUnit.Framework.Assert.AreEqual(fill, rec.startOffset);
			NUnit.Framework.Assert.AreEqual(fill, rec.rawLength);
			NUnit.Framework.Assert.AreEqual(fill, rec.partLength);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path f, long fill, int parts)
		{
			FSDataOutputStream @out = fs.Create(f, false);
			CheckedOutputStream iout = new CheckedOutputStream(@out, new CRC32());
			DataOutputStream dout = new DataOutputStream(iout);
			for (int i = 0; i < parts; ++i)
			{
				for (int j = 0; j < MapTask.MapOutputIndexRecordLength / 8; ++j)
				{
					dout.WriteLong(fill);
				}
			}
			@out.WriteLong(iout.GetChecksum().GetValue());
			dout.Close();
		}
	}
}
