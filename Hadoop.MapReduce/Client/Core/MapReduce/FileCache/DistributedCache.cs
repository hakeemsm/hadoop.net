using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Filecache
{
	/// <summary>Distribute application-specific large, read-only files efficiently.</summary>
	/// <remarks>
	/// Distribute application-specific large, read-only files efficiently.
	/// <p><code>DistributedCache</code> is a facility provided by the Map-Reduce
	/// framework to cache files (text, archives, jars etc.) needed by applications.
	/// </p>
	/// <p>Applications specify the files, via urls (hdfs:// or http://) to be cached
	/// via the
	/// <see cref="Org.Apache.Hadoop.Mapred.JobConf"/>
	/// . The
	/// <code>DistributedCache</code> assumes that the files specified via urls are
	/// already present on the
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// at the path specified by the url
	/// and are accessible by every machine in the cluster.</p>
	/// <p>The framework will copy the necessary files on to the slave node before
	/// any tasks for the job are executed on that node. Its efficiency stems from
	/// the fact that the files are only copied once per job and the ability to
	/// cache archives which are un-archived on the slaves.</p>
	/// <p><code>DistributedCache</code> can be used to distribute simple, read-only
	/// data/text files and/or more complex types such as archives, jars etc.
	/// Archives (zip, tar and tgz/tar.gz files) are un-archived at the slave nodes.
	/// Jars may be optionally added to the classpath of the tasks, a rudimentary
	/// software distribution mechanism.  Files have execution permissions.
	/// In older version of Hadoop Map/Reduce users could optionally ask for symlinks
	/// to be created in the working directory of the child task.  In the current
	/// version symlinks are always created.  If the URL does not have a fragment
	/// the name of the file or directory will be used. If multiple files or
	/// directories map to the same link name, the last one added, will be used.  All
	/// others will not even be downloaded.</p>
	/// <p><code>DistributedCache</code> tracks modification timestamps of the cache
	/// files. Clearly the cache files should not be modified by the application
	/// or externally while the job is executing.</p>
	/// <p>Here is an illustrative example on how to use the
	/// <code>DistributedCache</code>:</p>
	/// <p><blockquote><pre>
	/// // Setting up the cache for the application
	/// 1. Copy the requisite files to the <code>FileSystem</code>:
	/// $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat
	/// $ bin/hadoop fs -copyFromLocal map.zip /myapp/map.zip
	/// $ bin/hadoop fs -copyFromLocal mylib.jar /myapp/mylib.jar
	/// $ bin/hadoop fs -copyFromLocal mytar.tar /myapp/mytar.tar
	/// $ bin/hadoop fs -copyFromLocal mytgz.tgz /myapp/mytgz.tgz
	/// $ bin/hadoop fs -copyFromLocal mytargz.tar.gz /myapp/mytargz.tar.gz
	/// 2. Setup the application's <code>JobConf</code>:
	/// JobConf job = new JobConf();
	/// DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"),
	/// job);
	/// DistributedCache.addCacheArchive(new URI("/myapp/map.zip", job);
	/// DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
	/// DistributedCache.addCacheArchive(new URI("/myapp/mytar.tar", job);
	/// DistributedCache.addCacheArchive(new URI("/myapp/mytgz.tgz", job);
	/// DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
	/// 3. Use the cached files in the
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// or
	/// <see cref="Org.Apache.Hadoop.Mapred.Reducer{K2, V2, K3, V3}"/>
	/// :
	/// public static class MapClass extends MapReduceBase
	/// implements Mapper&lt;K, V, K, V&gt; {
	/// private Path[] localArchives;
	/// private Path[] localFiles;
	/// public void configure(JobConf job) {
	/// // Get the cached archives/files
	/// File f = new File("./map.zip/some/file/in/zip.txt");
	/// }
	/// public void map(K key, V value,
	/// OutputCollector&lt;K, V&gt; output, Reporter reporter)
	/// throws IOException {
	/// // Use data from the cached archives/files here
	/// // ...
	/// // ...
	/// output.collect(k, v);
	/// }
	/// }
	/// </pre></blockquote>
	/// It is also very common to use the DistributedCache by using
	/// <see cref="Org.Apache.Hadoop.Util.GenericOptionsParser"/>
	/// .
	/// This class includes methods that should be used by users
	/// (specifically those mentioned in the example above, as well
	/// as
	/// <see cref="AddArchiveToClassPath(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.Conf.Configuration)
	/// 	"/>
	/// ),
	/// as well as methods intended for use by the MapReduce framework
	/// (e.g.,
	/// <see cref="Org.Apache.Hadoop.Mapred.JobClient"/>
	/// ).
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Mapred.JobConf"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapred.JobClient"/>
	public class DistributedCache
	{
		/// <summary>Set the configuration with the given set of archives.</summary>
		/// <remarks>
		/// Set the configuration with the given set of archives.  Intended
		/// to be used by user code.
		/// </remarks>
		/// <param name="archives">The list of archives that need to be localized</param>
		/// <param name="conf">Configuration which will be changed</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.SetCacheArchives(Sharpen.URI[]) instead"
			)]
		public static void SetCacheArchives(URI[] archives, Configuration conf)
		{
			string sarchives = StringUtils.UriToString(archives);
			conf.Set(MRJobConfig.CacheArchives, sarchives);
		}

		/// <summary>Set the configuration with the given set of files.</summary>
		/// <remarks>
		/// Set the configuration with the given set of files.  Intended to be
		/// used by user code.
		/// </remarks>
		/// <param name="files">The list of files that need to be localized</param>
		/// <param name="conf">Configuration which will be changed</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.SetCacheFiles(Sharpen.URI[]) instead"
			)]
		public static void SetCacheFiles(URI[] files, Configuration conf)
		{
			string sfiles = StringUtils.UriToString(files);
			conf.Set(MRJobConfig.CacheFiles, sfiles);
		}

		/// <summary>Get cache archives set in the Configuration.</summary>
		/// <remarks>
		/// Get cache archives set in the Configuration.  Used by
		/// internal DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which contains the archives</param>
		/// <returns>A URI array of the caches set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetCacheArchives() instead"
			)]
		public static URI[] GetCacheArchives(Configuration conf)
		{
			return StringUtils.StringToURI(conf.GetStrings(MRJobConfig.CacheArchives));
		}

		/// <summary>Get cache files set in the Configuration.</summary>
		/// <remarks>
		/// Get cache files set in the Configuration.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which contains the files</param>
		/// <returns>A URI array of the files set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetCacheFiles() instead"
			)]
		public static URI[] GetCacheFiles(Configuration conf)
		{
			return StringUtils.StringToURI(conf.GetStrings(MRJobConfig.CacheFiles));
		}

		/// <summary>Return the path array of the localized caches.</summary>
		/// <remarks>
		/// Return the path array of the localized caches.  Intended to be used
		/// by user code.
		/// </remarks>
		/// <param name="conf">Configuration that contains the localized archives</param>
		/// <returns>A path array of localized caches</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetLocalCacheArchives() instead"
			)]
		public static Path[] GetLocalCacheArchives(Configuration conf)
		{
			return StringUtils.StringToPath(conf.GetStrings(MRJobConfig.CacheLocalarchives));
		}

		/// <summary>Return the path array of the localized files.</summary>
		/// <remarks>
		/// Return the path array of the localized files.  Intended to be used
		/// by user code.
		/// </remarks>
		/// <param name="conf">Configuration that contains the localized files</param>
		/// <returns>A path array of localized files</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetLocalCacheFiles() instead"
			)]
		public static Path[] GetLocalCacheFiles(Configuration conf)
		{
			return StringUtils.StringToPath(conf.GetStrings(MRJobConfig.CacheLocalfiles));
		}

		/// <summary>Parse a list of strings into longs.</summary>
		/// <param name="strs">the list of strings to parse</param>
		/// <returns>a list of longs that were parsed. same length as strs.</returns>
		private static long[] ParseTimestamps(string[] strs)
		{
			if (strs == null)
			{
				return null;
			}
			long[] result = new long[strs.Length];
			for (int i = 0; i < strs.Length; ++i)
			{
				result[i] = long.Parse(strs[i]);
			}
			return result;
		}

		/// <summary>Get the timestamps of the archives.</summary>
		/// <remarks>
		/// Get the timestamps of the archives.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which stored the timestamps</param>
		/// <returns>a long array of timestamps</returns>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetArchiveTimestamps() instead"
			)]
		public static long[] GetArchiveTimestamps(Configuration conf)
		{
			return ParseTimestamps(conf.GetStrings(MRJobConfig.CacheArchivesTimestamps));
		}

		/// <summary>Get the timestamps of the files.</summary>
		/// <remarks>
		/// Get the timestamps of the files.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which stored the timestamps</param>
		/// <returns>a long array of timestamps</returns>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetFileTimestamps() instead"
			)]
		public static long[] GetFileTimestamps(Configuration conf)
		{
			return ParseTimestamps(conf.GetStrings(MRJobConfig.CacheFileTimestamps));
		}

		/// <summary>Add a archives to be localized to the conf.</summary>
		/// <remarks>
		/// Add a archives to be localized to the conf.  Intended to
		/// be used by user code.
		/// </remarks>
		/// <param name="uri">The uri of the cache to be localized</param>
		/// <param name="conf">Configuration to add the cache to</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.AddCacheArchive(Sharpen.URI) instead"
			)]
		public static void AddCacheArchive(URI uri, Configuration conf)
		{
			string archives = conf.Get(MRJobConfig.CacheArchives);
			conf.Set(MRJobConfig.CacheArchives, archives == null ? uri.ToString() : archives 
				+ "," + uri.ToString());
		}

		/// <summary>Add a file to be localized to the conf.</summary>
		/// <remarks>
		/// Add a file to be localized to the conf.  Intended
		/// to be used by user code.
		/// </remarks>
		/// <param name="uri">The uri of the cache to be localized</param>
		/// <param name="conf">Configuration to add the cache to</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.AddCacheFile(Sharpen.URI) instead"
			)]
		public static void AddCacheFile(URI uri, Configuration conf)
		{
			string files = conf.Get(MRJobConfig.CacheFiles);
			conf.Set(MRJobConfig.CacheFiles, files == null ? uri.ToString() : files + "," + uri
				.ToString());
		}

		/// <summary>
		/// Add an file path to the current set of classpath entries It adds the file
		/// to cache as well.
		/// </summary>
		/// <remarks>
		/// Add an file path to the current set of classpath entries It adds the file
		/// to cache as well.  Intended to be used by user code.
		/// </remarks>
		/// <param name="file">Path of the file to be added</param>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.AddFileToClassPath(Org.Apache.Hadoop.FS.Path) instead"
			)]
		public static void AddFileToClassPath(Path file, Configuration conf)
		{
			AddFileToClassPath(file, conf, file.GetFileSystem(conf));
		}

		/// <summary>Add a file path to the current set of classpath entries.</summary>
		/// <remarks>
		/// Add a file path to the current set of classpath entries. It adds the file
		/// to cache as well.  Intended to be used by user code.
		/// </remarks>
		/// <param name="file">Path of the file to be added</param>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		/// <param name="fs">
		/// FileSystem with respect to which
		/// <c>archivefile</c>
		/// should
		/// be interpreted.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void AddFileToClassPath(Path file, Configuration conf, FileSystem fs
			)
		{
			string classpath = conf.Get(MRJobConfig.ClasspathFiles);
			conf.Set(MRJobConfig.ClasspathFiles, classpath == null ? file.ToString() : classpath
				 + "," + file.ToString());
			URI uri = fs.MakeQualified(file).ToUri();
			AddCacheFile(uri, conf);
		}

		/// <summary>Get the file entries in classpath as an array of Path.</summary>
		/// <remarks>
		/// Get the file entries in classpath as an array of Path.
		/// Used by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetFileClassPaths() instead"
			)]
		public static Path[] GetFileClassPaths(Configuration conf)
		{
			AList<string> list = (AList<string>)conf.GetStringCollection(MRJobConfig.ClasspathFiles
				);
			if (list.Count == 0)
			{
				return null;
			}
			Path[] paths = new Path[list.Count];
			for (int i = 0; i < list.Count; i++)
			{
				paths[i] = new Path(list[i]);
			}
			return paths;
		}

		/// <summary>Add an archive path to the current set of classpath entries.</summary>
		/// <remarks>
		/// Add an archive path to the current set of classpath entries. It adds the
		/// archive to cache as well.  Intended to be used by user code.
		/// </remarks>
		/// <param name="archive">Path of the archive to be added</param>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Job.AddArchiveToClassPath(Org.Apache.Hadoop.FS.Path) instead"
			)]
		public static void AddArchiveToClassPath(Path archive, Configuration conf)
		{
			AddArchiveToClassPath(archive, conf, archive.GetFileSystem(conf));
		}

		/// <summary>Add an archive path to the current set of classpath entries.</summary>
		/// <remarks>
		/// Add an archive path to the current set of classpath entries. It adds the
		/// archive to cache as well.  Intended to be used by user code.
		/// </remarks>
		/// <param name="archive">Path of the archive to be added</param>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		/// <param name="fs">
		/// FileSystem with respect to which
		/// <paramref name="archive"/>
		/// should be interpreted.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void AddArchiveToClassPath(Path archive, Configuration conf, FileSystem
			 fs)
		{
			string classpath = conf.Get(MRJobConfig.ClasspathArchives);
			conf.Set(MRJobConfig.ClasspathArchives, classpath == null ? archive.ToString() : 
				classpath + "," + archive.ToString());
			URI uri = fs.MakeQualified(archive).ToUri();
			AddCacheArchive(uri, conf);
		}

		/// <summary>Get the archive entries in classpath as an array of Path.</summary>
		/// <remarks>
		/// Get the archive entries in classpath as an array of Path.
		/// Used by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">Configuration that contains the classpath setting</param>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.JobContext.GetArchiveClassPaths() instead"
			)]
		public static Path[] GetArchiveClassPaths(Configuration conf)
		{
			AList<string> list = (AList<string>)conf.GetStringCollection(MRJobConfig.ClasspathArchives
				);
			if (list.Count == 0)
			{
				return null;
			}
			Path[] paths = new Path[list.Count];
			for (int i = 0; i < list.Count; i++)
			{
				paths[i] = new Path(list[i]);
			}
			return paths;
		}

		/// <summary>
		/// Originally intended to enable symlinks, but currently symlinks cannot be
		/// disabled.
		/// </summary>
		/// <remarks>
		/// Originally intended to enable symlinks, but currently symlinks cannot be
		/// disabled. This is a NO-OP.
		/// </remarks>
		/// <param name="conf">the jobconf</param>
		[System.ObsoleteAttribute(@"This is a NO-OP.")]
		public static void CreateSymlink(Configuration conf)
		{
		}

		//NOOP
		/// <summary>
		/// Originally intended to check if symlinks should be used, but currently
		/// symlinks cannot be disabled.
		/// </summary>
		/// <param name="conf">the jobconf</param>
		/// <returns>true</returns>
		[System.ObsoleteAttribute(@"symlinks are always created.")]
		public static bool GetSymlink(Configuration conf)
		{
			return true;
		}

		private static bool[] ParseBooleans(string[] strs)
		{
			if (null == strs)
			{
				return null;
			}
			bool[] result = new bool[strs.Length];
			for (int i = 0; i < strs.Length; ++i)
			{
				result[i] = System.Boolean.Parse(strs[i]);
			}
			return result;
		}

		/// <summary>Get the booleans on whether the files are public or not.</summary>
		/// <remarks>
		/// Get the booleans on whether the files are public or not.  Used by
		/// internal DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which stored the timestamps</param>
		/// <returns>a string array of booleans</returns>
		public static bool[] GetFileVisibilities(Configuration conf)
		{
			return ParseBooleans(conf.GetStrings(MRJobConfig.CacheFileVisibilities));
		}

		/// <summary>Get the booleans on whether the archives are public or not.</summary>
		/// <remarks>
		/// Get the booleans on whether the archives are public or not.  Used by
		/// internal DistributedCache and MapReduce code.
		/// </remarks>
		/// <param name="conf">The configuration which stored the timestamps</param>
		/// <returns>a string array of booleans</returns>
		public static bool[] GetArchiveVisibilities(Configuration conf)
		{
			return ParseBooleans(conf.GetStrings(MRJobConfig.CacheArchivesVisibilities));
		}

		/// <summary>
		/// This method checks if there is a conflict in the fragment names
		/// of the uris.
		/// </summary>
		/// <remarks>
		/// This method checks if there is a conflict in the fragment names
		/// of the uris. Also makes sure that each uri has a fragment. It
		/// is only to be called if you want to create symlinks for
		/// the various archives and files.  May be used by user code.
		/// </remarks>
		/// <param name="uriFiles">The uri array of urifiles</param>
		/// <param name="uriArchives">the uri array of uri archives</param>
		public static bool CheckURIs(URI[] uriFiles, URI[] uriArchives)
		{
			if ((uriFiles == null) && (uriArchives == null))
			{
				return true;
			}
			// check if fragment is null for any uri
			// also check if there are any conflicts in fragment names
			ICollection<string> fragments = new HashSet<string>();
			// iterate over file uris
			if (uriFiles != null)
			{
				for (int i = 0; i < uriFiles.Length; i++)
				{
					string fragment = uriFiles[i].GetFragment();
					if (fragment == null)
					{
						return false;
					}
					string lowerCaseFragment = StringUtils.ToLowerCase(fragment);
					if (fragments.Contains(lowerCaseFragment))
					{
						return false;
					}
					fragments.AddItem(lowerCaseFragment);
				}
			}
			// iterate over archive uris
			if (uriArchives != null)
			{
				for (int i = 0; i < uriArchives.Length; i++)
				{
					string fragment = uriArchives[i].GetFragment();
					if (fragment == null)
					{
						return false;
					}
					string lowerCaseFragment = StringUtils.ToLowerCase(fragment);
					if (fragments.Contains(lowerCaseFragment))
					{
						return false;
					}
					fragments.AddItem(lowerCaseFragment);
				}
			}
			return true;
		}
	}
}
