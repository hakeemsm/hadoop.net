using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJobName : ClusterMapReduceTestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestComplexName()
		{
			OutputStream os = GetFileSystem().Create(new Path(GetInputDir(), "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("b a\n");
			wr.Close();
			JobConf conf = CreateJobConf();
			conf.SetJobName("[name][some other value that gets truncated internally that this test attempts to aggravate]"
				);
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			FileInputFormat.SetInputPaths(conf, GetInputDir());
			FileOutputFormat.SetOutputPath(conf, GetOutputDir());
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(GetOutputDir(
				), new Utils.OutputFileUtils.OutputFilesFilter()));
			NUnit.Framework.Assert.AreEqual(1, outputFiles.Length);
			InputStream @is = GetFileSystem().Open(outputFiles[0]);
			BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
			NUnit.Framework.Assert.AreEqual("0\tb a", reader.ReadLine());
			NUnit.Framework.Assert.IsNull(reader.ReadLine());
			reader.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestComplexNameWithRegex()
		{
			OutputStream os = GetFileSystem().Create(new Path(GetInputDir(), "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("b a\n");
			wr.Close();
			JobConf conf = CreateJobConf();
			conf.SetJobName("name \\Evalue]");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			FileInputFormat.SetInputPaths(conf, GetInputDir());
			FileOutputFormat.SetOutputPath(conf, GetOutputDir());
			JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(GetOutputDir(
				), new Utils.OutputFileUtils.OutputFilesFilter()));
			NUnit.Framework.Assert.AreEqual(1, outputFiles.Length);
			InputStream @is = GetFileSystem().Open(outputFiles[0]);
			BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
			NUnit.Framework.Assert.AreEqual("0\tb a", reader.ReadLine());
			NUnit.Framework.Assert.IsNull(reader.ReadLine());
			reader.Close();
		}
	}
}
