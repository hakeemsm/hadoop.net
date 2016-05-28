using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A helper class for managing the distributed cache for
	/// <see cref="LocalJobRunner"/>
	/// .
	/// </summary>
	internal class LocalDistributedCacheManager
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(LocalDistributedCacheManager
			));

		private IList<string> localArchives = new AList<string>();

		private IList<string> localFiles = new AList<string>();

		private IList<string> localClasspaths = new AList<string>();

		private IList<FilePath> symlinksCreated = new AList<FilePath>();

		private bool setupCalled = false;

		/// <summary>
		/// Set up the distributed cache by localizing the resources, and updating
		/// the configuration with references to the localized resources.
		/// </summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Setup(JobConf conf)
		{
			FilePath workDir = new FilePath(Runtime.GetProperty("user.dir"));
			// Generate YARN local resources objects corresponding to the distributed
			// cache configuration
			IDictionary<string, LocalResource> localResources = new LinkedHashMap<string, LocalResource
				>();
			MRApps.SetupDistributedCache(conf, localResources);
			// Generating unique numbers for FSDownload.
			AtomicLong uniqueNumberGenerator = new AtomicLong(Runtime.CurrentTimeMillis());
			// Find which resources are to be put on the local classpath
			IDictionary<string, Path> classpaths = new Dictionary<string, Path>();
			Path[] archiveClassPaths = DistributedCache.GetArchiveClassPaths(conf);
			if (archiveClassPaths != null)
			{
				foreach (Path p in archiveClassPaths)
				{
					classpaths[p.ToUri().GetPath().ToString()] = p;
				}
			}
			Path[] fileClassPaths = DistributedCache.GetFileClassPaths(conf);
			if (fileClassPaths != null)
			{
				foreach (Path p in fileClassPaths)
				{
					classpaths[p.ToUri().GetPath().ToString()] = p;
				}
			}
			// Localize the resources
			LocalDirAllocator localDirAllocator = new LocalDirAllocator(MRConfig.LocalDir);
			FileContext localFSFileContext = FileContext.GetLocalFSFileContext();
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			ExecutorService exec = null;
			try
			{
				ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("LocalDistributedCacheManager Downloader #%d"
					).Build();
				exec = Executors.NewCachedThreadPool(tf);
				Path destPath = localDirAllocator.GetLocalPathForWrite(".", conf);
				IDictionary<LocalResource, Future<Path>> resourcesToPaths = Maps.NewHashMap();
				foreach (LocalResource resource in localResources.Values)
				{
					Callable<Path> download = new FSDownload(localFSFileContext, ugi, conf, new Path(
						destPath, System.Convert.ToString(uniqueNumberGenerator.IncrementAndGet())), resource
						);
					Future<Path> future = exec.Submit(download);
					resourcesToPaths[resource] = future;
				}
				foreach (KeyValuePair<string, LocalResource> entry in localResources)
				{
					LocalResource resource_1 = entry.Value;
					Path path;
					try
					{
						path = resourcesToPaths[resource_1].Get();
					}
					catch (Exception e)
					{
						throw new IOException(e);
					}
					catch (ExecutionException e)
					{
						throw new IOException(e);
					}
					string pathString = path.ToUri().ToString();
					string link = entry.Key;
					string target = new FilePath(path.ToUri()).GetPath();
					Symlink(workDir, target, link);
					if (resource_1.GetType() == LocalResourceType.Archive)
					{
						localArchives.AddItem(pathString);
					}
					else
					{
						if (resource_1.GetType() == LocalResourceType.File)
						{
							localFiles.AddItem(pathString);
						}
						else
						{
							if (resource_1.GetType() == LocalResourceType.Pattern)
							{
								//PATTERN is not currently used in local mode
								throw new ArgumentException("Resource type PATTERN is not " + "implemented yet. "
									 + resource_1.GetResource());
							}
						}
					}
					Path resourcePath;
					try
					{
						resourcePath = ConverterUtils.GetPathFromYarnURL(resource_1.GetResource());
					}
					catch (URISyntaxException e)
					{
						throw new IOException(e);
					}
					Log.Info(string.Format("Localized %s as %s", resourcePath, path));
					string cp = resourcePath.ToUri().GetPath();
					if (classpaths.Keys.Contains(cp))
					{
						localClasspaths.AddItem(path.ToUri().GetPath().ToString());
					}
				}
			}
			finally
			{
				if (exec != null)
				{
					exec.Shutdown();
				}
			}
			// Update the configuration object with localized data.
			if (!localArchives.IsEmpty())
			{
				conf.Set(MRJobConfig.CacheLocalarchives, StringUtils.ArrayToString(Sharpen.Collections.ToArray
					(localArchives, new string[localArchives.Count])));
			}
			if (!localFiles.IsEmpty())
			{
				conf.Set(MRJobConfig.CacheLocalfiles, StringUtils.ArrayToString(Sharpen.Collections.ToArray
					(localFiles, new string[localArchives.Count])));
			}
			setupCalled = true;
		}

		/// <summary>Utility method for creating a symlink and warning on errors.</summary>
		/// <remarks>
		/// Utility method for creating a symlink and warning on errors.
		/// If link is null, does nothing.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Symlink(FilePath workDir, string target, string link)
		{
			if (link != null)
			{
				link = workDir.ToString() + Path.Separator + link;
				FilePath flink = new FilePath(link);
				if (!flink.Exists())
				{
					Log.Info(string.Format("Creating symlink: %s <- %s", target, link));
					if (0 != FileUtil.SymLink(target, link))
					{
						Log.Warn(string.Format("Failed to create symlink: %s <- %s", target, link));
					}
					else
					{
						symlinksCreated.AddItem(new FilePath(link));
					}
				}
			}
		}

		/// <summary>
		/// Are the resources that should be added to the classpath?
		/// Should be called after setup().
		/// </summary>
		public virtual bool HasLocalClasspaths()
		{
			if (!setupCalled)
			{
				throw new InvalidOperationException("hasLocalClasspaths() should be called after setup()"
					);
			}
			return !localClasspaths.IsEmpty();
		}

		/// <summary>
		/// Creates a class loader that includes the designated
		/// files and archives.
		/// </summary>
		/// <exception cref="System.UriFormatException"/>
		public virtual ClassLoader MakeClassLoader(ClassLoader parent)
		{
			Uri[] urls = new Uri[localClasspaths.Count];
			for (int i = 0; i < localClasspaths.Count; ++i)
			{
				urls[i] = new FilePath(localClasspaths[i]).ToURI().ToURL();
				Log.Info(urls[i]);
			}
			return AccessController.DoPrivileged(new _PrivilegedAction_236(urls, parent));
		}

		private sealed class _PrivilegedAction_236 : PrivilegedAction<ClassLoader>
		{
			public _PrivilegedAction_236(Uri[] urls, ClassLoader parent)
			{
				this.urls = urls;
				this.parent = parent;
			}

			public ClassLoader Run()
			{
				return new URLClassLoader(urls, parent);
			}

			private readonly Uri[] urls;

			private readonly ClassLoader parent;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			foreach (FilePath symlink in symlinksCreated)
			{
				if (!symlink.Delete())
				{
					Log.Warn("Failed to delete symlink created by the local job runner: " + symlink);
				}
			}
			FileContext localFSFileContext = FileContext.GetLocalFSFileContext();
			foreach (string archive in localArchives)
			{
				localFSFileContext.Delete(new Path(archive), true);
			}
			foreach (string file in localFiles)
			{
				localFSFileContext.Delete(new Path(file), true);
			}
		}
	}
}
