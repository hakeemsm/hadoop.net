using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestClusterMapReduceTestCase : ClusterMapReduceTestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void _testMapReduce(bool restart)
		{
			OutputStream os = GetFileSystem().Create(new Path(GetInputDir(), "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("hello1\n");
			wr.Write("hello2\n");
			wr.Write("hello3\n");
			wr.Write("hello4\n");
			wr.Close();
			if (restart)
			{
				StopCluster();
				StartCluster(false, null);
			}
			JobConf conf = CreateJobConf();
			conf.SetJobName("mr");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			FileInputFormat.SetInputPaths(conf, GetInputDir());
			FileOutputFormat.SetOutputPath(conf, GetOutputDir());
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(GetOutputDir(
				), new Utils.OutputFileUtils.OutputFilesFilter()));
			if (outputFiles.Length > 0)
			{
				InputStream @is = GetFileSystem().Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				int counter = 0;
				while (line != null)
				{
					counter++;
					NUnit.Framework.Assert.IsTrue(line.Contains("hello"));
					line = reader.ReadLine();
				}
				reader.Close();
				NUnit.Framework.Assert.AreEqual(4, counter);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapReduce()
		{
			_testMapReduce(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapReduceRestarting()
		{
			_testMapReduce(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDFSRestart()
		{
			Path file = new Path(GetInputDir(), "text.txt");
			OutputStream os = GetFileSystem().Create(file);
			TextWriter wr = new OutputStreamWriter(os);
			wr.Close();
			StopCluster();
			StartCluster(false, null);
			NUnit.Framework.Assert.IsTrue(GetFileSystem().Exists(file));
			StopCluster();
			StartCluster(true, null);
			NUnit.Framework.Assert.IsFalse(GetFileSystem().Exists(file));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMRConfig()
		{
			JobConf conf = CreateJobConf();
			NUnit.Framework.Assert.IsNull(conf.Get("xyz"));
			Properties config = new Properties();
			config.SetProperty("xyz", "XYZ");
			StopCluster();
			StartCluster(false, config);
			conf = CreateJobConf();
			NUnit.Framework.Assert.AreEqual("XYZ", conf.Get("xyz"));
		}
	}
}
