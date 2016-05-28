using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A JUnit test to test Mini Map-Reduce Cluster with multiple directories
	/// and check for correct classpath
	/// </summary>
	public class TestMiniMRClasspath
	{
		/// <exception cref="System.IO.IOException"/>
		internal static void ConfigureWordCount(FileSystem fs, JobConf conf, string input
			, int numMaps, int numReduces, Path inDir, Path outDir)
		{
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes(input);
			file.Close();
			FileSystem.SetDefaultUri(conf, fs.GetUri());
			conf.Set(JTConfig.FrameworkName, JTConfig.YarnFrameworkName);
			conf.SetJobName("wordcount");
			conf.SetInputFormat(typeof(TextInputFormat));
			// the keys are words (strings)
			conf.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			conf.SetOutputValueClass(typeof(IntWritable));
			conf.Set("mapred.mapper.class", "testjar.ClassWordCount$MapClass");
			conf.Set("mapred.combine.class", "testjar.ClassWordCount$Reduce");
			conf.Set("mapred.reducer.class", "testjar.ClassWordCount$Reduce");
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReduces);
			//set the tests jar file
			conf.SetJarByClass(typeof(TestMiniMRClasspath));
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string LaunchWordCount(URI fileSys, JobConf conf, string input, int
			 numMaps, int numReduces)
		{
			Path inDir = new Path("/testing/wc/input");
			Path outDir = new Path("/testing/wc/output");
			FileSystem fs = FileSystem.Get(fileSys, conf);
			ConfigureWordCount(fs, conf, input, numMaps, numReduces, inDir, outDir);
			JobClient.RunJob(conf);
			StringBuilder result = new StringBuilder();
			{
				Path[] parents = FileUtil.Stat2Paths(fs.ListStatus(outDir.GetParent()));
				Path[] fileList = FileUtil.Stat2Paths(fs.ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
					()));
				for (int i = 0; i < fileList.Length; ++i)
				{
					BufferedReader file = new BufferedReader(new InputStreamReader(fs.Open(fileList[i
						])));
					string line = file.ReadLine();
					while (line != null)
					{
						result.Append(line);
						result.Append("\n");
						line = file.ReadLine();
					}
					file.Close();
				}
			}
			return result.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string LaunchExternal(URI uri, JobConf conf, string input, int numMaps
			, int numReduces)
		{
			Path inDir = new Path("/testing/ext/input");
			Path outDir = new Path("/testing/ext/output");
			FileSystem fs = FileSystem.Get(uri, conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			FileSystem.SetDefaultUri(conf, uri);
			conf.Set(JTConfig.FrameworkName, JTConfig.YarnFrameworkName);
			conf.SetJobName("wordcount");
			conf.SetInputFormat(typeof(TextInputFormat));
			// the keys are counts
			conf.SetOutputValueClass(typeof(IntWritable));
			// the values are the messages
			conf.Set(JobContext.OutputKeyClass, "testjar.ExternalWritable");
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReduces);
			conf.Set("mapred.mapper.class", "testjar.ExternalMapperReducer");
			conf.Set("mapred.reducer.class", "testjar.ExternalMapperReducer");
			// set the tests jar file
			conf.SetJarByClass(typeof(TestMiniMRClasspath));
			JobClient.RunJob(conf);
			StringBuilder result = new StringBuilder();
			Path[] fileList = FileUtil.Stat2Paths(fs.ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			for (int i = 0; i < fileList.Length; ++i)
			{
				BufferedReader file = new BufferedReader(new InputStreamReader(fs.Open(fileList[i
					])));
				string line = file.ReadLine();
				while (line != null)
				{
					result.Append(line);
					line = file.ReadLine();
					result.Append("\n");
				}
				file.Close();
			}
			return result.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClassPath()
		{
			string namenode = null;
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				int taskTrackers = 4;
				int jobTrackerPort = 60050;
				Configuration conf = new Configuration();
				dfs = new MiniDFSCluster.Builder(conf).Build();
				fileSys = dfs.GetFileSystem();
				namenode = fileSys.GetUri().ToString();
				mr = new MiniMRCluster(taskTrackers, namenode, 3);
				JobConf jobConf = mr.CreateJobConf();
				string result;
				result = LaunchWordCount(fileSys.GetUri(), jobConf, "The quick brown fox\nhas many silly\n"
					 + "red fox sox\n", 3, 1);
				NUnit.Framework.Assert.AreEqual("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" + "quick\t1\nred\t1\nsilly\t1\nsox\t1\n"
					, result);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
				if (mr != null)
				{
					mr.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestExternalWritable()
		{
			string namenode = null;
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				int taskTrackers = 4;
				Configuration conf = new Configuration();
				dfs = new MiniDFSCluster.Builder(conf).Build();
				fileSys = dfs.GetFileSystem();
				namenode = fileSys.GetUri().ToString();
				mr = new MiniMRCluster(taskTrackers, namenode, 3);
				JobConf jobConf = mr.CreateJobConf();
				string result;
				result = LaunchExternal(fileSys.GetUri(), jobConf, "Dennis was here!\nDennis again!"
					, 3, 1);
				NUnit.Framework.Assert.AreEqual("Dennis again!\t1\nDennis was here!\t1\n", result
					);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
				if (mr != null)
				{
					mr.Shutdown();
				}
			}
		}
	}
}
