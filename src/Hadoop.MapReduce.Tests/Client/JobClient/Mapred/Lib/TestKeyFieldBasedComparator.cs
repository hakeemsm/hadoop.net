using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestKeyFieldBasedComparator : HadoopTestCase
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestKeyFieldBasedComparator-lib");

		internal JobConf conf;

		internal JobConf localConf;

		internal string line1 = "123 -123 005120 123.9 0.01 0.18 010 10.0 4444.1 011 011 234";

		internal string line2 = "134 -12 005100 123.10 -1.01 0.19 02 10.1 4444";

		/// <exception cref="System.IO.IOException"/>
		public TestKeyFieldBasedComparator()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
			line1_bytes = Sharpen.Runtime.GetBytesForString(line1);
			line2_bytes = Sharpen.Runtime.GetBytesForString(line2);
			conf = CreateJobConf();
			localConf = CreateJobConf();
			localConf.Set(JobContext.MapOutputKeyFieldSeperator, " ");
		}

		/// <exception cref="System.Exception"/>
		public virtual void Configure(string keySpec, int expect)
		{
			Path testdir = new Path(TestDir.GetAbsolutePath());
			Path inDir = new Path(testdir, "in");
			Path outDir = new Path(testdir, "out");
			FileSystem fs = GetFileSystem();
			fs.Delete(testdir, true);
			conf.SetInputFormat(typeof(TextInputFormat));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetOutputKeyClass(typeof(Text));
			conf.SetOutputValueClass(typeof(LongWritable));
			conf.SetNumMapTasks(1);
			conf.SetNumReduceTasks(1);
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyComparatorClass(typeof(KeyFieldBasedComparator));
			conf.SetKeyFieldComparatorOptions(keySpec);
			conf.SetKeyFieldPartitionerOptions("-k1.1,1.1");
			conf.Set(JobContext.MapOutputKeyFieldSeperator, " ");
			conf.SetMapperClass(typeof(InverseMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			if (!fs.Mkdirs(testdir))
			{
				throw new IOException("Mkdirs failed to create " + testdir.ToString());
			}
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			// set up input data in 2 files 
			Path inFile = new Path(inDir, "part0");
			FileOutputStream fos = new FileOutputStream(inFile.ToString());
			fos.Write(Sharpen.Runtime.GetBytesForString((line1 + "\n")));
			fos.Write(Sharpen.Runtime.GetBytesForString((line2 + "\n")));
			fos.Close();
			JobClient jc = new JobClient(conf);
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (!r_job.IsSuccessful())
			{
				Fail("Oops! The job broke due to an unexpected error");
			}
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			if (outputFiles.Length > 0)
			{
				InputStream @is = GetFileSystem().Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				//make sure we get what we expect as the first line, and also
				//that we have two lines
				if (expect == 1)
				{
					NUnit.Framework.Assert.IsTrue(line.StartsWith(line1));
				}
				else
				{
					if (expect == 2)
					{
						NUnit.Framework.Assert.IsTrue(line.StartsWith(line2));
					}
				}
				line = reader.ReadLine();
				if (expect == 1)
				{
					NUnit.Framework.Assert.IsTrue(line.StartsWith(line2));
				}
				else
				{
					if (expect == 2)
					{
						NUnit.Framework.Assert.IsTrue(line.StartsWith(line1));
					}
				}
				reader.Close();
			}
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(TestDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicUnixComparator()
		{
			Configure("-k1,1n", 1);
			Configure("-k2,2n", 1);
			Configure("-k2.2,2n", 2);
			Configure("-k3.4,3n", 2);
			Configure("-k3.2,3.3n -k4,4n", 2);
			Configure("-k3.2,3.3n -k4,4nr", 1);
			Configure("-k2.4,2.4n", 2);
			Configure("-k7,7", 1);
			Configure("-k7,7n", 2);
			Configure("-k8,8n", 1);
			Configure("-k9,9", 2);
			Configure("-k11,11", 2);
			Configure("-k10,10", 2);
			LocalTestWithoutMRJob("-k9,9", 1);
		}

		internal byte[] line1_bytes;

		internal byte[] line2_bytes;

		/// <exception cref="System.Exception"/>
		public virtual void LocalTestWithoutMRJob(string keySpec, int expect)
		{
			KeyFieldBasedComparator<Void, Void> keyFieldCmp = new KeyFieldBasedComparator<Void
				, Void>();
			localConf.SetKeyFieldComparatorOptions(keySpec);
			keyFieldCmp.Configure(localConf);
			int result = keyFieldCmp.Compare(line1_bytes, 0, line1_bytes.Length, line2_bytes, 
				0, line2_bytes.Length);
			if ((expect >= 0 && result < 0) || (expect < 0 && result >= 0))
			{
				Fail();
			}
		}
	}
}
