using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// check for the job submission options of
	/// -jt local -libjars
	/// </summary>
	public class TestLocalJobSubmission
	{
		private static Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", 
			"/tmp"));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Configure()
		{
		}

		[TearDown]
		public virtual void Cleanup()
		{
		}

		/// <summary>
		/// test the local job submission options of
		/// -jt local -libjars
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalJobLibjarsOption()
		{
			Path jarPath = MakeJar(new Path(TestRootDir, "test.jar"));
			Configuration conf = new Configuration();
			conf.Set(FileSystem.FsDefaultNameKey, "hdfs://testcluster");
			string[] args = new string[] { "-jt", "local", "-libjars", jarPath.ToString(), "-m"
				, "1", "-r", "1", "-mt", "1", "-rt", "1" };
			int res = -1;
			try
			{
				res = ToolRunner.Run(conf, new SleepJob(), args);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Job failed with " + e.GetLocalizedMessage());
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				NUnit.Framework.Assert.Fail("Job failed");
			}
			NUnit.Framework.Assert.AreEqual("dist job res is not 0:", 0, res);
		}

		/// <exception cref="System.IO.IOException"/>
		private Path MakeJar(Path p)
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(p.ToString()));
			JarOutputStream jos = new JarOutputStream(fos);
			ZipEntry ze = new ZipEntry("test.jar.inside");
			jos.PutNextEntry(ze);
			jos.Write(Sharpen.Runtime.GetBytesForString(("inside the jar!")));
			jos.CloseEntry();
			jos.Close();
			return p;
		}
	}
}
