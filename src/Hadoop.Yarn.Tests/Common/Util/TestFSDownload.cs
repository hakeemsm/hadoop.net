using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Com.Google.Common.Cache;
using NUnit.Framework;
using Org.Apache.Commons.Compress.Archivers.Tar;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestFSDownload
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFSDownload));

		private static AtomicLong uniqueNumberGenerator = new AtomicLong(Runtime.CurrentTimeMillis
			());

		private enum TEST_FILE_TYPE
		{
			Tar,
			Jar,
			Zip,
			Tgz
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void DeleteTestDir()
		{
			FileContext fs = FileContext.GetLocalFSFileContext();
			fs.Delete(new Path("target", typeof(TestFSDownload).Name), true);
		}

		internal static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.IO.IOException"/>
		internal static LocalResource CreateFile(FileContext files, Path p, int len, Random
			 r, LocalResourceVisibility vis)
		{
			CreateFile(files, p, len, r);
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(p));
			ret.SetSize(len);
			ret.SetType(LocalResourceType.File);
			ret.SetVisibility(vis);
			ret.SetTimestamp(files.GetFileStatus(p).GetModificationTime());
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CreateFile(FileContext files, Path p, int len, Random r)
		{
			FSDataOutputStream @out = null;
			try
			{
				byte[] bytes = new byte[len];
				@out = files.Create(p, EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite));
				r.NextBytes(bytes);
				@out.Write(bytes);
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static LocalResource CreateJar(FileContext files, Path p, LocalResourceVisibility
			 vis)
		{
			Log.Info("Create jar file " + p);
			FilePath jarFile = new FilePath((files.MakeQualified(p)).ToUri());
			FileOutputStream stream = new FileOutputStream(jarFile);
			Log.Info("Create jar out stream ");
			JarOutputStream @out = new JarOutputStream(stream, new Manifest());
			Log.Info("Done writing jar stream ");
			@out.Close();
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(p));
			FileStatus status = files.GetFileStatus(p);
			ret.SetSize(status.GetLen());
			ret.SetTimestamp(status.GetModificationTime());
			ret.SetType(LocalResourceType.Pattern);
			ret.SetVisibility(vis);
			ret.SetPattern("classes/.*");
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalResource CreateTarFile(FileContext files, Path p, int len, Random
			 r, LocalResourceVisibility vis)
		{
			byte[] bytes = new byte[len];
			r.NextBytes(bytes);
			FilePath archiveFile = new FilePath(p.ToUri().GetPath() + ".tar");
			archiveFile.CreateNewFile();
			TarArchiveOutputStream @out = new TarArchiveOutputStream(new FileOutputStream(archiveFile
				));
			TarArchiveEntry entry = new TarArchiveEntry(p.GetName());
			entry.SetSize(bytes.Length);
			@out.PutArchiveEntry(entry);
			@out.Write(bytes);
			@out.CloseArchiveEntry();
			@out.Close();
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(new Path(p.ToString() + ".tar")
				));
			ret.SetSize(len);
			ret.SetType(LocalResourceType.Archive);
			ret.SetVisibility(vis);
			ret.SetTimestamp(files.GetFileStatus(new Path(p.ToString() + ".tar")).GetModificationTime
				());
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalResource CreateTgzFile(FileContext files, Path p, int len, Random
			 r, LocalResourceVisibility vis)
		{
			byte[] bytes = new byte[len];
			r.NextBytes(bytes);
			FilePath gzipFile = new FilePath(p.ToUri().GetPath() + ".tar.gz");
			gzipFile.CreateNewFile();
			TarArchiveOutputStream @out = new TarArchiveOutputStream(new GZIPOutputStream(new 
				FileOutputStream(gzipFile)));
			TarArchiveEntry entry = new TarArchiveEntry(p.GetName());
			entry.SetSize(bytes.Length);
			@out.PutArchiveEntry(entry);
			@out.Write(bytes);
			@out.CloseArchiveEntry();
			@out.Close();
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(new Path(p.ToString() + ".tar.gz"
				)));
			ret.SetSize(len);
			ret.SetType(LocalResourceType.Archive);
			ret.SetVisibility(vis);
			ret.SetTimestamp(files.GetFileStatus(new Path(p.ToString() + ".tar.gz")).GetModificationTime
				());
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalResource CreateJarFile(FileContext files, Path p, int len, Random
			 r, LocalResourceVisibility vis)
		{
			byte[] bytes = new byte[len];
			r.NextBytes(bytes);
			FilePath archiveFile = new FilePath(p.ToUri().GetPath() + ".jar");
			archiveFile.CreateNewFile();
			JarOutputStream @out = new JarOutputStream(new FileOutputStream(archiveFile));
			@out.PutNextEntry(new JarEntry(p.GetName()));
			@out.Write(bytes);
			@out.CloseEntry();
			@out.Close();
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(new Path(p.ToString() + ".jar")
				));
			ret.SetSize(len);
			ret.SetType(LocalResourceType.Archive);
			ret.SetVisibility(vis);
			ret.SetTimestamp(files.GetFileStatus(new Path(p.ToString() + ".jar")).GetModificationTime
				());
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalResource CreateZipFile(FileContext files, Path p, int len, Random
			 r, LocalResourceVisibility vis)
		{
			byte[] bytes = new byte[len];
			r.NextBytes(bytes);
			FilePath archiveFile = new FilePath(p.ToUri().GetPath() + ".ZIP");
			archiveFile.CreateNewFile();
			ZipOutputStream @out = new ZipOutputStream(new FileOutputStream(archiveFile));
			@out.PutNextEntry(new ZipEntry(p.GetName()));
			@out.Write(bytes);
			@out.CloseEntry();
			@out.Close();
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(new Path(p.ToString() + ".ZIP")
				));
			ret.SetSize(len);
			ret.SetType(LocalResourceType.Archive);
			ret.SetVisibility(vis);
			ret.SetTimestamp(files.GetFileStatus(new Path(p.ToString() + ".ZIP")).GetModificationTime
				());
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadBadPublic()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			IDictionary<LocalResource, LocalResourceVisibility> rsrcVis = new Dictionary<LocalResource
				, LocalResourceVisibility>();
			Random rand = new Random();
			long sharedSeed = rand.NextLong();
			rand.SetSeed(sharedSeed);
			System.Console.Out.WriteLine("SEED: " + sharedSeed);
			IDictionary<LocalResource, Future<Path>> pending = new Dictionary<LocalResource, 
				Future<Path>>();
			ExecutorService exec = Executors.NewSingleThreadExecutor();
			LocalDirAllocator dirs = new LocalDirAllocator(typeof(TestFSDownload).FullName);
			int size = 512;
			LocalResourceVisibility vis = LocalResourceVisibility.Public;
			Path path = new Path(basedir, "test-file");
			LocalResource rsrc = CreateFile(files, path, size, rand, vis);
			rsrcVis[rsrc] = vis;
			Path destPath = dirs.GetLocalPathForWrite(basedir.ToString(), size, conf);
			destPath = new Path(destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
				()));
			FSDownload fsd = new FSDownload(files, UserGroupInformation.GetCurrentUser(), conf
				, destPath, rsrc);
			pending[rsrc] = exec.Submit(fsd);
			exec.Shutdown();
			while (!exec.AwaitTermination(1000, TimeUnit.Milliseconds))
			{
			}
			NUnit.Framework.Assert.IsTrue(pending[rsrc].IsDone());
			try
			{
				foreach (KeyValuePair<LocalResource, Future<Path>> p in pending)
				{
					p.Value.Get();
					NUnit.Framework.Assert.Fail("We localized a file that is not public.");
				}
			}
			catch (ExecutionException e)
			{
				NUnit.Framework.Assert.IsTrue(e.InnerException is IOException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual void TestDownloadPublicWithStatCache()
		{
			Configuration conf = new Configuration();
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			// if test directory doesn't have ancestor permission, skip this test
			FileSystem f = basedir.GetFileSystem(conf);
			Assume.AssumeTrue(FSDownload.AncestorsHaveExecutePermissions(f, basedir, null));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			int size = 512;
			ConcurrentMap<Path, AtomicInteger> counts = new ConcurrentHashMap<Path, AtomicInteger
				>();
			CacheLoader<Path, Future<FileStatus>> loader = FSDownload.CreateStatusCacheLoader
				(conf);
			LoadingCache<Path, Future<FileStatus>> statCache = CacheBuilder.NewBuilder().Build
				(new _CacheLoader_328(counts, loader));
			// increment the count
			// use the default loader
			// test FSDownload.isPublic() concurrently
			int fileCount = 3;
			IList<Callable<bool>> tasks = new AList<Callable<bool>>();
			for (int i = 0; i < fileCount; i++)
			{
				Random rand = new Random();
				long sharedSeed = rand.NextLong();
				rand.SetSeed(sharedSeed);
				System.Console.Out.WriteLine("SEED: " + sharedSeed);
				Path path = new Path(basedir, "test-file-" + i);
				CreateFile(files, path, size, rand);
				FileSystem fs = path.GetFileSystem(conf);
				FileStatus sStat = fs.GetFileStatus(path);
				tasks.AddItem(new _Callable_358(fs, path, sStat, statCache));
			}
			ExecutorService exec = Executors.NewFixedThreadPool(fileCount);
			try
			{
				IList<Future<bool>> futures = exec.InvokeAll(tasks);
				// files should be public
				foreach (Future<bool> future in futures)
				{
					NUnit.Framework.Assert.IsTrue(future.Get());
				}
				// for each path exactly one file status call should be made
				foreach (AtomicInteger count in counts.Values)
				{
					NUnit.Framework.Assert.AreSame(count.Get(), 1);
				}
			}
			finally
			{
				exec.Shutdown();
			}
		}

		private sealed class _CacheLoader_328 : CacheLoader<Path, Future<FileStatus>>
		{
			public _CacheLoader_328(ConcurrentMap<Path, AtomicInteger> counts, CacheLoader<Path
				, Future<FileStatus>> loader)
			{
				this.counts = counts;
				this.loader = loader;
			}

			/// <exception cref="System.Exception"/>
			public override Future<FileStatus> Load(Path path)
			{
				AtomicInteger count = counts[path];
				if (count == null)
				{
					count = new AtomicInteger(0);
					AtomicInteger existing = counts.PutIfAbsent(path, count);
					if (existing != null)
					{
						count = existing;
					}
				}
				count.IncrementAndGet();
				return loader.Load(path);
			}

			private readonly ConcurrentMap<Path, AtomicInteger> counts;

			private readonly CacheLoader<Path, Future<FileStatus>> loader;
		}

		private sealed class _Callable_358 : Callable<bool>
		{
			public _Callable_358(FileSystem fs, Path path, FileStatus sStat, LoadingCache<Path
				, Future<FileStatus>> statCache)
			{
				this.fs = fs;
				this.path = path;
				this.sStat = sStat;
				this.statCache = statCache;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Call()
			{
				return FSDownload.IsPublic(fs, path, sStat, statCache);
			}

			private readonly FileSystem fs;

			private readonly Path path;

			private readonly FileStatus sStat;

			private readonly LoadingCache<Path, Future<FileStatus>> statCache;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownload()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			IDictionary<LocalResource, LocalResourceVisibility> rsrcVis = new Dictionary<LocalResource
				, LocalResourceVisibility>();
			Random rand = new Random();
			long sharedSeed = rand.NextLong();
			rand.SetSeed(sharedSeed);
			System.Console.Out.WriteLine("SEED: " + sharedSeed);
			IDictionary<LocalResource, Future<Path>> pending = new Dictionary<LocalResource, 
				Future<Path>>();
			ExecutorService exec = Executors.NewSingleThreadExecutor();
			LocalDirAllocator dirs = new LocalDirAllocator(typeof(TestFSDownload).FullName);
			int[] sizes = new int[10];
			for (int i = 0; i < 10; ++i)
			{
				sizes[i] = rand.Next(512) + 512;
				LocalResourceVisibility vis = LocalResourceVisibility.Private;
				if (i % 2 == 1)
				{
					vis = LocalResourceVisibility.Application;
				}
				Path p = new Path(basedir, string.Empty + i);
				LocalResource rsrc = CreateFile(files, p, sizes[i], rand, vis);
				rsrcVis[rsrc] = vis;
				Path destPath = dirs.GetLocalPathForWrite(basedir.ToString(), sizes[i], conf);
				destPath = new Path(destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
					()));
				FSDownload fsd = new FSDownload(files, UserGroupInformation.GetCurrentUser(), conf
					, destPath, rsrc);
				pending[rsrc] = exec.Submit(fsd);
			}
			exec.Shutdown();
			while (!exec.AwaitTermination(1000, TimeUnit.Milliseconds))
			{
			}
			foreach (Future<Path> path in pending.Values)
			{
				NUnit.Framework.Assert.IsTrue(path.IsDone());
			}
			try
			{
				foreach (KeyValuePair<LocalResource, Future<Path>> p in pending)
				{
					Path localized = p.Value.Get();
					NUnit.Framework.Assert.AreEqual(sizes[Sharpen.Extensions.ValueOf(localized.GetName
						())], p.Key.GetSize());
					FileStatus status = files.GetFileStatus(localized.GetParent());
					FsPermission perm = status.GetPermission();
					NUnit.Framework.Assert.AreEqual("Cache directory permissions are incorrect", new 
						FsPermission((short)0x1ed), perm);
					status = files.GetFileStatus(localized);
					perm = status.GetPermission();
					System.Console.Out.WriteLine("File permission " + perm + " for rsrc vis " + p.Key
						.GetVisibility().ToString());
					System.Diagnostics.Debug.Assert((rsrcVis.Contains(p.Key)));
					NUnit.Framework.Assert.IsTrue("Private file should be 500", perm.ToShort() == FSDownload
						.PrivateFilePerms.ToShort());
				}
			}
			catch (ExecutionException e)
			{
				throw new IOException("Failed exec", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		private void DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE fileType)
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			Random rand = new Random();
			long sharedSeed = rand.NextLong();
			rand.SetSeed(sharedSeed);
			System.Console.Out.WriteLine("SEED: " + sharedSeed);
			IDictionary<LocalResource, Future<Path>> pending = new Dictionary<LocalResource, 
				Future<Path>>();
			ExecutorService exec = Executors.NewSingleThreadExecutor();
			LocalDirAllocator dirs = new LocalDirAllocator(typeof(TestFSDownload).FullName);
			int size = rand.Next(512) + 512;
			LocalResourceVisibility vis = LocalResourceVisibility.Private;
			Path p = new Path(basedir, string.Empty + 1);
			string strFileName = string.Empty;
			LocalResource rsrc = null;
			switch (fileType)
			{
				case TestFSDownload.TEST_FILE_TYPE.Tar:
				{
					rsrc = CreateTarFile(files, p, size, rand, vis);
					break;
				}

				case TestFSDownload.TEST_FILE_TYPE.Jar:
				{
					rsrc = CreateJarFile(files, p, size, rand, vis);
					rsrc.SetType(LocalResourceType.Pattern);
					break;
				}

				case TestFSDownload.TEST_FILE_TYPE.Zip:
				{
					rsrc = CreateZipFile(files, p, size, rand, vis);
					strFileName = p.GetName() + ".ZIP";
					break;
				}

				case TestFSDownload.TEST_FILE_TYPE.Tgz:
				{
					rsrc = CreateTgzFile(files, p, size, rand, vis);
					break;
				}
			}
			Path destPath = dirs.GetLocalPathForWrite(basedir.ToString(), size, conf);
			destPath = new Path(destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
				()));
			FSDownload fsd = new FSDownload(files, UserGroupInformation.GetCurrentUser(), conf
				, destPath, rsrc);
			pending[rsrc] = exec.Submit(fsd);
			exec.Shutdown();
			while (!exec.AwaitTermination(1000, TimeUnit.Milliseconds))
			{
			}
			try
			{
				pending[rsrc].Get();
				// see if there was an Exception during download
				FileStatus[] filesstatus = files.GetDefaultFileSystem().ListStatus(basedir);
				foreach (FileStatus filestatus in filesstatus)
				{
					if (filestatus.IsDirectory())
					{
						FileStatus[] childFiles = files.GetDefaultFileSystem().ListStatus(filestatus.GetPath
							());
						foreach (FileStatus childfile in childFiles)
						{
							if (strFileName.EndsWith(".ZIP") && childfile.GetPath().GetName().Equals(strFileName
								) && !childfile.IsDirectory())
							{
								NUnit.Framework.Assert.Fail("Failure...After unzip, there should have been a" + " directory formed with zip file name but found a file. "
									 + childfile.GetPath());
							}
							if (childfile.GetPath().GetName().StartsWith("tmp"))
							{
								NUnit.Framework.Assert.Fail("Tmp File should not have been there " + childfile.GetPath
									());
							}
						}
					}
				}
			}
			catch (Exception e)
			{
				throw new IOException("Failed exec", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadArchive()
		{
			DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE.Tar);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadPatternJar()
		{
			DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE.Jar);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadArchiveZip()
		{
			DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE.Zip);
		}

		/*
		* To test fix for YARN-3029
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadArchiveZipWithTurkishLocale()
		{
			CultureInfo defaultLocale = CultureInfo.CurrentCulture;
			// Set to Turkish
			CultureInfo turkishLocale = new CultureInfo("tr", "TR");
			System.Threading.Thread.CurrentThread.CurrentCulture = turkishLocale;
			DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE.Zip);
			// Set the locale back to original default locale
			System.Threading.Thread.CurrentThread.CurrentCulture = defaultLocale;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadArchiveTgz()
		{
			DownloadWithFileType(TestFSDownload.TEST_FILE_TYPE.Tgz);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyPermsRecursively(FileSystem fs, FileContext files, Path p, LocalResourceVisibility
			 vis)
		{
			FileStatus status = files.GetFileStatus(p);
			if (status.IsDirectory())
			{
				if (vis == LocalResourceVisibility.Public)
				{
					NUnit.Framework.Assert.IsTrue(status.GetPermission().ToShort() == FSDownload.PublicDirPerms
						.ToShort());
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(status.GetPermission().ToShort() == FSDownload.PrivateDirPerms
						.ToShort());
				}
				if (!status.IsSymlink())
				{
					FileStatus[] statuses = fs.ListStatus(p);
					foreach (FileStatus stat in statuses)
					{
						VerifyPermsRecursively(fs, files, stat.GetPath(), vis);
					}
				}
			}
			else
			{
				if (vis == LocalResourceVisibility.Public)
				{
					NUnit.Framework.Assert.IsTrue(status.GetPermission().ToShort() == FSDownload.PublicFilePerms
						.ToShort());
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(status.GetPermission().ToShort() == FSDownload.PrivateFilePerms
						.ToShort());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDirDownload()
		{
			Configuration conf = new Configuration();
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			IDictionary<LocalResource, LocalResourceVisibility> rsrcVis = new Dictionary<LocalResource
				, LocalResourceVisibility>();
			Random rand = new Random();
			long sharedSeed = rand.NextLong();
			rand.SetSeed(sharedSeed);
			System.Console.Out.WriteLine("SEED: " + sharedSeed);
			IDictionary<LocalResource, Future<Path>> pending = new Dictionary<LocalResource, 
				Future<Path>>();
			ExecutorService exec = Executors.NewSingleThreadExecutor();
			LocalDirAllocator dirs = new LocalDirAllocator(typeof(TestFSDownload).FullName);
			for (int i = 0; i < 5; ++i)
			{
				LocalResourceVisibility vis = LocalResourceVisibility.Private;
				if (i % 2 == 1)
				{
					vis = LocalResourceVisibility.Application;
				}
				Path p = new Path(basedir, "dir" + i + ".jar");
				LocalResource rsrc = CreateJar(files, p, vis);
				rsrcVis[rsrc] = vis;
				Path destPath = dirs.GetLocalPathForWrite(basedir.ToString(), conf);
				destPath = new Path(destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
					()));
				FSDownload fsd = new FSDownload(files, UserGroupInformation.GetCurrentUser(), conf
					, destPath, rsrc);
				pending[rsrc] = exec.Submit(fsd);
			}
			exec.Shutdown();
			while (!exec.AwaitTermination(1000, TimeUnit.Milliseconds))
			{
			}
			foreach (Future<Path> path in pending.Values)
			{
				NUnit.Framework.Assert.IsTrue(path.IsDone());
			}
			try
			{
				foreach (KeyValuePair<LocalResource, Future<Path>> p in pending)
				{
					Path localized = p.Value.Get();
					FileStatus status = files.GetFileStatus(localized);
					System.Console.Out.WriteLine("Testing path " + localized);
					System.Diagnostics.Debug.Assert((status.IsDirectory()));
					System.Diagnostics.Debug.Assert((rsrcVis.Contains(p.Key)));
					VerifyPermsRecursively(localized.GetFileSystem(conf), files, localized, rsrcVis[p
						.Key]);
				}
			}
			catch (ExecutionException e)
			{
				throw new IOException("Failed exec", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUniqueDestinationPath()
		{
			Configuration conf = new Configuration();
			FileContext files = FileContext.GetLocalFSFileContext(conf);
			Path basedir = files.MakeQualified(new Path("target", typeof(TestFSDownload).Name
				));
			files.Mkdir(basedir, null, true);
			conf.SetStrings(typeof(TestFSDownload).FullName, basedir.ToString());
			ExecutorService singleThreadedExec = Executors.NewSingleThreadExecutor();
			LocalDirAllocator dirs = new LocalDirAllocator(typeof(TestFSDownload).FullName);
			Path destPath = dirs.GetLocalPathForWrite(basedir.ToString(), conf);
			destPath = new Path(destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet
				()));
			Path p = new Path(basedir, "dir" + 0 + ".jar");
			LocalResourceVisibility vis = LocalResourceVisibility.Private;
			LocalResource rsrc = CreateJar(files, p, vis);
			FSDownload fsd = new FSDownload(files, UserGroupInformation.GetCurrentUser(), conf
				, destPath, rsrc);
			Future<Path> rPath = singleThreadedExec.Submit(fsd);
			singleThreadedExec.Shutdown();
			while (!singleThreadedExec.AwaitTermination(1000, TimeUnit.Milliseconds))
			{
			}
			NUnit.Framework.Assert.IsTrue(rPath.IsDone());
			// Now FSDownload will not create a random directory to localize the
			// resource. Therefore the final localizedPath for the resource should be
			// destination directory (passed as an argument) + file name.
			NUnit.Framework.Assert.AreEqual(destPath, rPath.Get().GetParent());
		}
	}
}
