using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>
	/// This testcase tests that a JobConf without default values submits jobs
	/// properly and the JT applies its own default values to it to make the job
	/// run properly.
	/// </summary>
	public class TestNoDefaultsJobConf : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestNoDefaultsJobConf()
			: base(HadoopTestCase.ClusterMr, HadoopTestCase.DfsFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoDefaults()
		{
			JobConf configuration = new JobConf();
			NUnit.Framework.Assert.IsTrue(configuration.Get("hadoop.tmp.dir", null) != null);
			configuration = new JobConf(false);
			NUnit.Framework.Assert.IsTrue(configuration.Get("hadoop.tmp.dir", null) == null);
			Path inDir = new Path("testing/jobconf/input");
			Path outDir = new Path("testing/jobconf/output");
			OutputStream os = GetFileSystem().Create(new Path(inDir, "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("hello\n");
			wr.Write("hello\n");
			wr.Close();
			JobConf conf = new JobConf(false);
			conf.Set("fs.defaultFS", CreateJobConf().Get("fs.defaultFS"));
			conf.SetJobName("mr");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
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
				NUnit.Framework.Assert.AreEqual(2, counter);
			}
		}
	}
}
