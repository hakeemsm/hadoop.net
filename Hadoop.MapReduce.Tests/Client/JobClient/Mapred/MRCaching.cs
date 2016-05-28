using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class MRCaching
	{
		internal static string testStr = "This is a test file " + "used for testing caching "
			 + "jars, zip and normal files.";

		/// <summary>Using the wordcount example and adding caching to it.</summary>
		/// <remarks>
		/// Using the wordcount example and adding caching to it. The cache
		/// archives/files are set and then are checked in the map if they have been
		/// localized or not.
		/// </remarks>
		public class MapClass : MapReduceBase, Mapper<LongWritable, Text, Text, IntWritable
			>
		{
			internal JobConf conf;

			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			public override void Configure(JobConf jconf)
			{
				conf = jconf;
				try
				{
					Path[] localArchives = DistributedCache.GetLocalCacheArchives(conf);
					Path[] localFiles = DistributedCache.GetLocalCacheFiles(conf);
					// read the cached files (unzipped, unjarred and text)
					// and put it into a single file TEST_ROOT_DIR/test.txt
					string TestRootDir = jconf.Get("test.build.data", "/tmp");
					Path file = new Path("file:///", TestRootDir);
					FileSystem fs = FileSystem.GetLocal(conf);
					if (!fs.Mkdirs(file))
					{
						throw new IOException("Mkdirs failed to create " + file.ToString());
					}
					Path fileOut = new Path(file, "test.txt");
					fs.Delete(fileOut, true);
					DataOutputStream @out = fs.Create(fileOut);
					for (int i = 0; i < localArchives.Length; i++)
					{
						// read out the files from these archives
						FilePath f = new FilePath(localArchives[i].ToString());
						FilePath txt = new FilePath(f, "test.txt");
						FileInputStream fin = new FileInputStream(txt);
						DataInputStream din = new DataInputStream(fin);
						string str = din.ReadLine();
						din.Close();
						@out.WriteBytes(str);
						@out.WriteBytes("\n");
					}
					for (int i_1 = 0; i_1 < localFiles.Length; i_1++)
					{
						// read out the files from these archives
						FilePath txt = new FilePath(localFiles[i_1].ToString());
						FileInputStream fin = new FileInputStream(txt);
						DataInputStream din = new DataInputStream(fin);
						string str = din.ReadLine();
						@out.WriteBytes(str);
						@out.WriteBytes("\n");
					}
					@out.Close();
				}
				catch (IOException ie)
				{
					System.Console.Out.WriteLine(StringUtils.StringifyException(ie));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, IntWritable
				> output, Reporter reporter)
			{
				string line = value.ToString();
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					output.Collect(word, one);
				}
			}
		}

		/// <summary>Using the wordcount example and adding caching to it.</summary>
		/// <remarks>
		/// Using the wordcount example and adding caching to it. The cache
		/// archives/files are set and then are checked in the map if they have been
		/// symlinked or not.
		/// </remarks>
		public class MapClass2 : MRCaching.MapClass
		{
			internal JobConf conf;

			public override void Configure(JobConf jconf)
			{
				conf = jconf;
				try
				{
					// read the cached files (unzipped, unjarred and text)
					// and put it into a single file TEST_ROOT_DIR/test.txt
					string TestRootDir = jconf.Get("test.build.data", "/tmp");
					Path file = new Path("file:///", TestRootDir);
					FileSystem fs = FileSystem.GetLocal(conf);
					if (!fs.Mkdirs(file))
					{
						throw new IOException("Mkdirs failed to create " + file.ToString());
					}
					Path fileOut = new Path(file, "test.txt");
					fs.Delete(fileOut, true);
					DataOutputStream @out = fs.Create(fileOut);
					string[] symlinks = new string[6];
					symlinks[0] = ".";
					symlinks[1] = "testjar";
					symlinks[2] = "testzip";
					symlinks[3] = "testtgz";
					symlinks[4] = "testtargz";
					symlinks[5] = "testtar";
					for (int i = 0; i < symlinks.Length; i++)
					{
						// read out the files from these archives
						FilePath f = new FilePath(symlinks[i]);
						FilePath txt = new FilePath(f, "test.txt");
						FileInputStream fin = new FileInputStream(txt);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
						string str = reader.ReadLine();
						reader.Close();
						@out.WriteBytes(str);
						@out.WriteBytes("\n");
					}
					@out.Close();
				}
				catch (IOException ie)
				{
					System.Console.Out.WriteLine(StringUtils.StringifyException(ie));
				}
			}
		}

		/// <summary>A reducer class that just emits the sum of the input values.</summary>
		public class ReduceClass : MapReduceBase, Reducer<Text, IntWritable, Text, IntWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Text key, IEnumerator<IntWritable> values, OutputCollector
				<Text, IntWritable> output, Reporter reporter)
			{
				int sum = 0;
				while (values.HasNext())
				{
					sum += values.Next().Get();
				}
				output.Collect(key, new IntWritable(sum));
			}
		}

		public class TestResult
		{
			public RunningJob job;

			public bool isOutputOk;

			internal TestResult(RunningJob job, bool isOutputOk)
			{
				this.job = job;
				this.isOutputOk = isOutputOk;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void SetupCache(string cacheDir, FileSystem fs)
		{
			Path localPath = new Path(Runtime.GetProperty("test.cache.data", "build/test/cache"
				));
			Path txtPath = new Path(localPath, new Path("test.txt"));
			Path jarPath = new Path(localPath, new Path("test.jar"));
			Path zipPath = new Path(localPath, new Path("test.zip"));
			Path tarPath = new Path(localPath, new Path("test.tgz"));
			Path tarPath1 = new Path(localPath, new Path("test.tar.gz"));
			Path tarPath2 = new Path(localPath, new Path("test.tar"));
			Path cachePath = new Path(cacheDir);
			fs.Delete(cachePath, true);
			if (!fs.Mkdirs(cachePath))
			{
				throw new IOException("Mkdirs failed to create " + cachePath.ToString());
			}
			fs.CopyFromLocalFile(txtPath, cachePath);
			fs.CopyFromLocalFile(jarPath, cachePath);
			fs.CopyFromLocalFile(zipPath, cachePath);
			fs.CopyFromLocalFile(tarPath, cachePath);
			fs.CopyFromLocalFile(tarPath1, cachePath);
			fs.CopyFromLocalFile(tarPath2, cachePath);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MRCaching.TestResult LaunchMRCache(string indir, string outdir, string
			 cacheDir, JobConf conf, string input)
		{
			string TestRootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).ToString
				().Replace(' ', '+');
			//if (TEST_ROOT_DIR.startsWith("C:")) TEST_ROOT_DIR = "/tmp";
			conf.Set("test.build.data", TestRootDir);
			Path inDir = new Path(indir);
			Path outDir = new Path(outdir);
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				System.Console.Out.WriteLine("HERE:" + inDir);
				DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			conf.SetJobName("cachetest");
			// the keys are words (strings)
			conf.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			conf.SetOutputValueClass(typeof(IntWritable));
			conf.SetCombinerClass(typeof(MRCaching.ReduceClass));
			conf.SetReducerClass(typeof(MRCaching.ReduceClass));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(1);
			conf.SetNumReduceTasks(1);
			conf.SetSpeculativeExecution(false);
			URI[] uris = new URI[6];
			conf.SetMapperClass(typeof(MRCaching.MapClass2));
			uris[0] = fs.GetUri().Resolve(cacheDir + "/test.txt");
			uris[1] = fs.GetUri().Resolve(cacheDir + "/test.jar");
			uris[2] = fs.GetUri().Resolve(cacheDir + "/test.zip");
			uris[3] = fs.GetUri().Resolve(cacheDir + "/test.tgz");
			uris[4] = fs.GetUri().Resolve(cacheDir + "/test.tar.gz");
			uris[5] = fs.GetUri().Resolve(cacheDir + "/test.tar");
			DistributedCache.AddCacheFile(uris[0], conf);
			// Save expected file sizes
			long[] fileSizes = new long[1];
			fileSizes[0] = fs.GetFileStatus(new Path(uris[0].GetPath())).GetLen();
			long[] archiveSizes = new long[5];
			// track last 5
			for (int i = 1; i < 6; i++)
			{
				DistributedCache.AddCacheArchive(uris[i], conf);
				archiveSizes[i - 1] = fs.GetFileStatus(new Path(uris[i].GetPath())).GetLen();
			}
			// starting with second archive
			RunningJob job = JobClient.RunJob(conf);
			int count = 0;
			// after the job ran check to see if the input from the localized cache
			// match the real string. check if there are 3 instances or not.
			Path result = new Path(TestRootDir + "/test.txt");
			{
				BufferedReader file = new BufferedReader(new InputStreamReader(FileSystem.GetLocal
					(conf).Open(result)));
				string line = file.ReadLine();
				while (line != null)
				{
					if (!testStr.Equals(line))
					{
						return new MRCaching.TestResult(job, false);
					}
					count++;
					line = file.ReadLine();
				}
				file.Close();
			}
			if (count != 6)
			{
				return new MRCaching.TestResult(job, false);
			}
			// Check to ensure the filesizes of files in DC were correctly saved.
			// Note, the underlying job clones the original conf before determine
			// various stats (timestamps etc.), so we have to getConfiguration here.
			ValidateCacheFileSizes(job.GetConfiguration(), fileSizes, MRJobConfig.CacheFilesSizes
				);
			ValidateCacheFileSizes(job.GetConfiguration(), archiveSizes, MRJobConfig.CacheArchivesSizes
				);
			return new MRCaching.TestResult(job, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateCacheFileSizes(Configuration job, long[] expectedSizes
			, string configKey)
		{
			string configValues = job.Get(configKey, string.Empty);
			System.Console.Out.WriteLine(configKey + " -> " + configValues);
			string[] realSizes = StringUtils.GetStrings(configValues);
			NUnit.Framework.Assert.AreEqual("Number of files for " + configKey, expectedSizes
				.Length, realSizes.Length);
			for (int i = 0; i < expectedSizes.Length; ++i)
			{
				long actual = Sharpen.Extensions.ValueOf(realSizes[i]);
				long expected = expectedSizes[i];
				NUnit.Framework.Assert.AreEqual("File " + i + " for " + configKey, expected, actual
					);
			}
		}
	}
}
