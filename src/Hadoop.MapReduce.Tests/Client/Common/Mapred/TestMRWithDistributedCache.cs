using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Filecache;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Tests the use of the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
	/// within the
	/// full MR flow as well as the LocalJobRunner. This ought to be part of the
	/// filecache package, but that package is not currently in mapred, so cannot
	/// depend on MR for testing.
	/// We use the distributed.* namespace for temporary files.
	/// See
	/// <see cref="TestMiniMRLocalFS"/>
	/// ,
	/// <see cref="TestMiniMRDFSCaching"/>
	/// , and
	/// <see cref="MRCaching"/>
	/// for other tests that test the distributed cache.
	/// This test is not fast: it uses MiniMRCluster.
	/// </summary>
	public class TestMRWithDistributedCache : TestCase
	{
		private static Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", 
			"/tmp"));

		private static FilePath symlinkFile = new FilePath("distributed.first.symlink");

		private static FilePath expectedAbsentSymlinkFile = new FilePath("distributed.second.jar"
			);

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		static TestMRWithDistributedCache()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRWithDistributedCache
			));

		private class DistributedCacheChecker
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Setup<_T0>(TaskInputOutputContext<_T0> context)
			{
				Configuration conf = context.GetConfiguration();
				Path[] localFiles = context.GetLocalCacheFiles();
				URI[] files = context.GetCacheFiles();
				Path[] localArchives = context.GetLocalCacheArchives();
				URI[] archives = context.GetCacheArchives();
				FileSystem fs = LocalFileSystem.Get(conf);
				// Check that 2 files and 2 archives are present
				NUnit.Framework.Assert.AreEqual(2, localFiles.Length);
				NUnit.Framework.Assert.AreEqual(2, localArchives.Length);
				NUnit.Framework.Assert.AreEqual(2, files.Length);
				NUnit.Framework.Assert.AreEqual(2, archives.Length);
				// Check the file name
				NUnit.Framework.Assert.IsTrue(files[0].GetPath().EndsWith("distributed.first"));
				NUnit.Framework.Assert.IsTrue(files[1].GetPath().EndsWith("distributed.second.jar"
					));
				// Check lengths of the files
				NUnit.Framework.Assert.AreEqual(1, fs.GetFileStatus(localFiles[0]).GetLen());
				NUnit.Framework.Assert.IsTrue(fs.GetFileStatus(localFiles[1]).GetLen() > 1);
				// Check extraction of the archive
				NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(localArchives[0], "distributed.jar.inside3"
					)));
				NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(localArchives[1], "distributed.jar.inside4"
					)));
				// Check the class loaders
				Log.Info("Java Classpath: " + Runtime.GetProperty("java.class.path"));
				ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
				// Both the file and the archive were added to classpath, so both
				// should be reachable via the class loader.
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("distributed.jar.inside2"));
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("distributed.jar.inside3"));
				NUnit.Framework.Assert.IsNull(cl.GetResource("distributed.jar.inside4"));
				// Check that the symlink for the renaming was created in the cwd;
				NUnit.Framework.Assert.IsTrue("symlink distributed.first.symlink doesn't exist", 
					symlinkFile.Exists());
				NUnit.Framework.Assert.AreEqual("symlink distributed.first.symlink length not 1", 
					1, symlinkFile.Length());
				//This last one is a difference between MRv2 and MRv1
				NUnit.Framework.Assert.IsTrue("second file should be symlinked too", expectedAbsentSymlinkFile
					.Exists());
			}
		}

		public class DistributedCacheCheckerMapper : Mapper<LongWritable, Text, NullWritable
			, NullWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				new TestMRWithDistributedCache.DistributedCacheChecker().Setup(context);
			}
		}

		public class DistributedCacheCheckerReducer : Reducer<LongWritable, Text, NullWritable
			, NullWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void Setup(Reducer.Context context)
			{
				new TestMRWithDistributedCache.DistributedCacheChecker().Setup(context);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private void TestWithConf(Configuration conf)
		{
			// Create a temporary file of length 1.
			Path first = CreateTempFile("distributed.first", "x");
			// Create two jars with a single file inside them.
			Path second = MakeJar(new Path(TestRootDir, "distributed.second.jar"), 2);
			Path third = MakeJar(new Path(TestRootDir, "distributed.third.jar"), 3);
			Path fourth = MakeJar(new Path(TestRootDir, "distributed.fourth.jar"), 4);
			Job job = Job.GetInstance(conf);
			job.SetMapperClass(typeof(TestMRWithDistributedCache.DistributedCacheCheckerMapper
				));
			job.SetReducerClass(typeof(TestMRWithDistributedCache.DistributedCacheCheckerReducer
				));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			FileInputFormat.SetInputPaths(job, first);
			// Creates the Job Configuration
			job.AddCacheFile(new URI(first.ToUri().ToString() + "#distributed.first.symlink")
				);
			job.AddFileToClassPath(second);
			job.AddArchiveToClassPath(third);
			job.AddCacheArchive(fourth.ToUri());
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.Submit();
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(false));
		}

		/// <summary>Tests using the local job runner.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLocalJobRunner()
		{
			symlinkFile.Delete();
			// ensure symlink is not present (e.g. if test is
			// killed part way through)
			Configuration c = new Configuration();
			c.Set(JTConfig.JtIpcAddress, "local");
			c.Set("fs.defaultFS", "file:///");
			TestWithConf(c);
			NUnit.Framework.Assert.IsFalse("Symlink not removed by local job runner", Arrays.
				AsList(new FilePath(".").List()).Contains(symlinkFile.GetName()));
		}

		// Symlink target will have gone so can't use File.exists()
		/// <exception cref="System.IO.IOException"/>
		private Path CreateTempFile(string filename, string contents)
		{
			Path path = new Path(TestRootDir, filename);
			FSDataOutputStream os = localFs.Create(path);
			os.WriteBytes(contents);
			os.Close();
			return path;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private Path MakeJar(Path p, int index)
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(p.ToString()));
			JarOutputStream jos = new JarOutputStream(fos);
			ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
			jos.PutNextEntry(ze);
			jos.Write(Sharpen.Runtime.GetBytesForString(("inside the jar!" + index)));
			jos.CloseEntry();
			jos.Close();
			return p;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeprecatedFunctions()
		{
			DistributedCache.AddLocalArchives(conf, "Test Local Archives 1");
			NUnit.Framework.Assert.AreEqual("Test Local Archives 1", conf.Get(DistributedCache
				.CacheLocalarchives));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetLocalCacheArchives(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Archives 1", DistributedCache.GetLocalCacheArchives
				(conf)[0].GetName());
			DistributedCache.AddLocalArchives(conf, "Test Local Archives 2");
			NUnit.Framework.Assert.AreEqual("Test Local Archives 1,Test Local Archives 2", conf
				.Get(DistributedCache.CacheLocalarchives));
			NUnit.Framework.Assert.AreEqual(2, DistributedCache.GetLocalCacheArchives(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Archives 2", DistributedCache.GetLocalCacheArchives
				(conf)[1].GetName());
			DistributedCache.SetLocalArchives(conf, "Test Local Archives 3");
			NUnit.Framework.Assert.AreEqual("Test Local Archives 3", conf.Get(DistributedCache
				.CacheLocalarchives));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetLocalCacheArchives(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Archives 3", DistributedCache.GetLocalCacheArchives
				(conf)[0].GetName());
			DistributedCache.AddLocalFiles(conf, "Test Local Files 1");
			NUnit.Framework.Assert.AreEqual("Test Local Files 1", conf.Get(DistributedCache.CacheLocalfiles
				));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetLocalCacheFiles(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Files 1", DistributedCache.GetLocalCacheFiles
				(conf)[0].GetName());
			DistributedCache.AddLocalFiles(conf, "Test Local Files 2");
			NUnit.Framework.Assert.AreEqual("Test Local Files 1,Test Local Files 2", conf.Get
				(DistributedCache.CacheLocalfiles));
			NUnit.Framework.Assert.AreEqual(2, DistributedCache.GetLocalCacheFiles(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Files 2", DistributedCache.GetLocalCacheFiles
				(conf)[1].GetName());
			DistributedCache.SetLocalFiles(conf, "Test Local Files 3");
			NUnit.Framework.Assert.AreEqual("Test Local Files 3", conf.Get(DistributedCache.CacheLocalfiles
				));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetLocalCacheFiles(conf).Length
				);
			NUnit.Framework.Assert.AreEqual("Test Local Files 3", DistributedCache.GetLocalCacheFiles
				(conf)[0].GetName());
			DistributedCache.SetArchiveTimestamps(conf, "1234567890");
			NUnit.Framework.Assert.AreEqual(1234567890, conf.GetLong(DistributedCache.CacheArchivesTimestamps
				, 0));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetArchiveTimestamps(conf).Length
				);
			NUnit.Framework.Assert.AreEqual(1234567890, DistributedCache.GetArchiveTimestamps
				(conf)[0]);
			DistributedCache.SetFileTimestamps(conf, "1234567890");
			NUnit.Framework.Assert.AreEqual(1234567890, conf.GetLong(DistributedCache.CacheFilesTimestamps
				, 0));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetFileTimestamps(conf).Length
				);
			NUnit.Framework.Assert.AreEqual(1234567890, DistributedCache.GetFileTimestamps(conf
				)[0]);
			DistributedCache.CreateAllSymlink(conf, new FilePath("Test Job Cache Dir"), new FilePath
				("Test Work Dir"));
			NUnit.Framework.Assert.IsNull(conf.Get(DistributedCache.CacheSymlink));
			NUnit.Framework.Assert.IsTrue(DistributedCache.GetSymlink(conf));
			NUnit.Framework.Assert.IsTrue(symlinkFile.CreateNewFile());
			FileStatus fileStatus = DistributedCache.GetFileStatus(conf, symlinkFile.ToURI());
			NUnit.Framework.Assert.IsNotNull(fileStatus);
			NUnit.Framework.Assert.AreEqual(fileStatus.GetModificationTime(), DistributedCache
				.GetTimestamp(conf, symlinkFile.ToURI()));
			NUnit.Framework.Assert.IsTrue(symlinkFile.Delete());
			DistributedCache.AddCacheArchive(symlinkFile.ToURI(), conf);
			NUnit.Framework.Assert.AreEqual(symlinkFile.ToURI().ToString(), conf.Get(DistributedCache
				.CacheArchives));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetCacheArchives(conf).Length
				);
			NUnit.Framework.Assert.AreEqual(symlinkFile.ToURI(), DistributedCache.GetCacheArchives
				(conf)[0]);
			DistributedCache.AddCacheFile(symlinkFile.ToURI(), conf);
			NUnit.Framework.Assert.AreEqual(symlinkFile.ToURI().ToString(), conf.Get(DistributedCache
				.CacheFiles));
			NUnit.Framework.Assert.AreEqual(1, DistributedCache.GetCacheFiles(conf).Length);
			NUnit.Framework.Assert.AreEqual(symlinkFile.ToURI(), DistributedCache.GetCacheFiles
				(conf)[0]);
		}
	}
}
