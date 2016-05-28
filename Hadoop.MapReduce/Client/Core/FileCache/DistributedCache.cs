using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Sharpen;

namespace Org.Apache.Hadoop.Filecache
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
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache.AddArchiveToClassPath(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.Conf.Configuration)
	/// 	"/>
	/// ),
	/// as well as methods intended for use by the MapReduce framework
	/// (e.g.,
	/// <see cref="Org.Apache.Hadoop.Mapred.JobClient"/>
	/// ).
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Mapred.JobConf"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapred.JobClient"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Job"/>
	public class DistributedCache : DistributedCache
	{
		/// <summary>
		/// Warning:
		/// <see cref="CacheFilesSizes"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheFilesSizes"/>
		/// </summary>
		[Obsolete]
		public const string CacheFilesSizes = "mapred.cache.files.filesizes";

		/// <summary>
		/// Warning:
		/// <see cref="CacheArchivesSizes"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheArchivesSizes"/>
		/// </summary>
		[Obsolete]
		public const string CacheArchivesSizes = "mapred.cache.archives.filesizes";

		/// <summary>
		/// Warning:
		/// <see cref="CacheArchivesTimestamps"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheArchivesTimestamps"/>
		/// </summary>
		[Obsolete]
		public const string CacheArchivesTimestamps = "mapred.cache.archives.timestamps";

		/// <summary>
		/// Warning:
		/// <see cref="CacheFilesTimestamps"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheFileTimestamps"/>
		/// </summary>
		[Obsolete]
		public const string CacheFilesTimestamps = "mapred.cache.files.timestamps";

		/// <summary>
		/// Warning:
		/// <see cref="CacheArchives"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheArchives"/>
		/// </summary>
		[Obsolete]
		public const string CacheArchives = "mapred.cache.archives";

		/// <summary>
		/// Warning:
		/// <see cref="CacheFiles"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheFiles"/>
		/// </summary>
		[Obsolete]
		public const string CacheFiles = "mapred.cache.files";

		/// <summary>
		/// Warning:
		/// <see cref="CacheLocalarchives"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheLocalarchives"/>
		/// </summary>
		[Obsolete]
		public const string CacheLocalarchives = "mapred.cache.localArchives";

		/// <summary>
		/// Warning:
		/// <see cref="CacheLocalfiles"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheLocalfiles"/>
		/// </summary>
		[Obsolete]
		public const string CacheLocalfiles = "mapred.cache.localFiles";

		/// <summary>
		/// Warning:
		/// <see cref="CacheSymlink"/>
		/// is not a *public* constant.
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.CacheSymlink"/>
		/// </summary>
		[Obsolete]
		public const string CacheSymlink = "mapred.create.symlink";

		/// <summary>Add a archive that has been localized to the conf.</summary>
		/// <remarks>
		/// Add a archive that has been localized to the conf.  Used
		/// by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">The conf to modify to contain the localized caches</param>
		/// <param name="str">a comma separated list of local archives</param>
		[Obsolete]
		public static void AddLocalArchives(Configuration conf, string str)
		{
			string archives = conf.Get(CacheLocalarchives);
			conf.Set(CacheLocalarchives, archives == null ? str : archives + "," + str);
		}

		/// <summary>Add a file that has been localized to the conf..</summary>
		/// <remarks>
		/// Add a file that has been localized to the conf..  Used
		/// by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">The conf to modify to contain the localized caches</param>
		/// <param name="str">a comma separated list of local files</param>
		[Obsolete]
		public static void AddLocalFiles(Configuration conf, string str)
		{
			string files = conf.Get(CacheLocalfiles);
			conf.Set(CacheLocalfiles, files == null ? str : files + "," + str);
		}

		/// <summary>
		/// This method create symlinks for all files in a given dir in another
		/// directory.
		/// </summary>
		/// <remarks>
		/// This method create symlinks for all files in a given dir in another
		/// directory. Currently symlinks cannot be disabled. This is a NO-OP.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="jobCacheDir">the target directory for creating symlinks</param>
		/// <param name="workDir">the directory in which the symlinks are created</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Internal to MapReduce framework.  Use DistributedCacheManager instead."
			)]
		public static void CreateAllSymlink(Configuration conf, FilePath jobCacheDir, FilePath
			 workDir)
		{
		}

		// Do nothing
		/// <summary>
		/// Returns
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// of a given cache file on hdfs. Internal to
		/// MapReduce.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="cache">cache file</param>
		/// <returns><code>FileStatus</code> of a given cache file on hdfs</returns>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static FileStatus GetFileStatus(Configuration conf, URI cache)
		{
			FileSystem fileSystem = FileSystem.Get(cache, conf);
			return fileSystem.GetFileStatus(new Path(cache.GetPath()));
		}

		/// <summary>Returns mtime of a given cache file on hdfs.</summary>
		/// <remarks>Returns mtime of a given cache file on hdfs. Internal to MapReduce.</remarks>
		/// <param name="conf">configuration</param>
		/// <param name="cache">cache file</param>
		/// <returns>mtime of a given cache file on hdfs</returns>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static long GetTimestamp(Configuration conf, URI cache)
		{
			return GetFileStatus(conf, cache).GetModificationTime();
		}

		/// <summary>This is to check the timestamp of the archives to be localized.</summary>
		/// <remarks>
		/// This is to check the timestamp of the archives to be localized.
		/// Used by internal MapReduce code.
		/// </remarks>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="timestamps">
		/// comma separated list of timestamps of archives.
		/// The order should be the same as the order in which the archives are added.
		/// </param>
		[Obsolete]
		public static void SetArchiveTimestamps(Configuration conf, string timestamps)
		{
			conf.Set(CacheArchivesTimestamps, timestamps);
		}

		/// <summary>This is to check the timestamp of the files to be localized.</summary>
		/// <remarks>
		/// This is to check the timestamp of the files to be localized.
		/// Used by internal MapReduce code.
		/// </remarks>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="timestamps">
		/// comma separated list of timestamps of files.
		/// The order should be the same as the order in which the files are added.
		/// </param>
		[Obsolete]
		public static void SetFileTimestamps(Configuration conf, string timestamps)
		{
			conf.Set(CacheFilesTimestamps, timestamps);
		}

		/// <summary>Set the conf to contain the location for localized archives.</summary>
		/// <remarks>
		/// Set the conf to contain the location for localized archives.  Used
		/// by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">The conf to modify to contain the localized caches</param>
		/// <param name="str">a comma separated list of local archives</param>
		[Obsolete]
		public static void SetLocalArchives(Configuration conf, string str)
		{
			conf.Set(CacheLocalarchives, str);
		}

		/// <summary>Set the conf to contain the location for localized files.</summary>
		/// <remarks>
		/// Set the conf to contain the location for localized files.  Used
		/// by internal DistributedCache code.
		/// </remarks>
		/// <param name="conf">The conf to modify to contain the localized caches</param>
		/// <param name="str">a comma separated list of local files</param>
		[Obsolete]
		public static void SetLocalFiles(Configuration conf, string str)
		{
			conf.Set(CacheLocalfiles, str);
		}
	}
}
