using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Testshell;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// check for the job submission  options of
	/// -libjars -files -archives
	/// </summary>
	public class TestCommandLineJobSubmission : TestCase
	{
		internal static readonly Path input = new Path("/test/input/");

		internal static readonly Path output = new Path("/test/output");

		internal FilePath buildDir = new FilePath(Runtime.GetProperty("test.build.data", 
			"/tmp"));

		// Input output paths for this.. 
		// these are all dummy and does not test
		// much in map reduce except for the command line
		// params 
		/// <exception cref="System.Exception"/>
		public virtual void TestJobShell()
		{
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fs = null;
			Path testFile = new Path(input, "testfile");
			try
			{
				Configuration conf = new Configuration();
				//start the mini mr and dfs cluster.
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				fs = dfs.GetFileSystem();
				FSDataOutputStream stream = fs.Create(testFile);
				stream.Write(Sharpen.Runtime.GetBytesForString("teststring"));
				stream.Close();
				mr = new MiniMRCluster(2, fs.GetUri().ToString(), 1);
				FilePath thisbuildDir = new FilePath(buildDir, "jobCommand");
				NUnit.Framework.Assert.IsTrue("create build dir", thisbuildDir.Mkdirs());
				FilePath f = new FilePath(thisbuildDir, "files_tmp");
				FileOutputStream fstream = new FileOutputStream(f);
				fstream.Write(Sharpen.Runtime.GetBytesForString("somestrings"));
				fstream.Close();
				FilePath f1 = new FilePath(thisbuildDir, "files_tmp1");
				fstream = new FileOutputStream(f1);
				fstream.Write(Sharpen.Runtime.GetBytesForString("somestrings"));
				fstream.Close();
				// copy files to dfs
				Path cachePath = new Path("/cacheDir");
				if (!fs.Mkdirs(cachePath))
				{
					throw new IOException("Mkdirs failed to create " + cachePath.ToString());
				}
				Path localCachePath = new Path(Runtime.GetProperty("test.cache.data"));
				Path txtPath = new Path(localCachePath, new Path("test.txt"));
				Path jarPath = new Path(localCachePath, new Path("test.jar"));
				Path zipPath = new Path(localCachePath, new Path("test.zip"));
				Path tarPath = new Path(localCachePath, new Path("test.tar"));
				Path tgzPath = new Path(localCachePath, new Path("test.tgz"));
				fs.CopyFromLocalFile(txtPath, cachePath);
				fs.CopyFromLocalFile(jarPath, cachePath);
				fs.CopyFromLocalFile(zipPath, cachePath);
				// construct options for -files
				string[] files = new string[3];
				files[0] = f.ToString();
				files[1] = f1.ToString() + "#localfilelink";
				files[2] = fs.GetUri().Resolve(cachePath + "/test.txt#dfsfilelink").ToString();
				// construct options for -libjars
				string[] libjars = new string[2];
				libjars[0] = "build/test/mapred/testjar/testjob.jar";
				libjars[1] = fs.GetUri().Resolve(cachePath + "/test.jar").ToString();
				// construct options for archives
				string[] archives = new string[3];
				archives[0] = tgzPath.ToString();
				archives[1] = tarPath + "#tarlink";
				archives[2] = fs.GetUri().Resolve(cachePath + "/test.zip#ziplink").ToString();
				string[] args = new string[10];
				args[0] = "-files";
				args[1] = StringUtils.ArrayToString(files);
				args[2] = "-libjars";
				// the testjob.jar as a temporary jar file 
				// rather than creating its own
				args[3] = StringUtils.ArrayToString(libjars);
				args[4] = "-archives";
				args[5] = StringUtils.ArrayToString(archives);
				args[6] = "-D";
				args[7] = "mapred.output.committer.class=testjar.CustomOutputCommitter";
				args[8] = input.ToString();
				args[9] = output.ToString();
				JobConf jobConf = mr.CreateJobConf();
				//before running the job, verify that libjar is not in client classpath
				NUnit.Framework.Assert.IsTrue("libjar not in client classpath", LoadLibJar(jobConf
					) == null);
				int ret = ToolRunner.Run(jobConf, new ExternalMapReduce(), args);
				//after running the job, verify that libjar is in the client classpath
				NUnit.Framework.Assert.IsTrue("libjar added to client classpath", LoadLibJar(jobConf
					) != null);
				NUnit.Framework.Assert.IsTrue("not failed ", ret != -1);
				f.Delete();
				thisbuildDir.Delete();
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

		private Type LoadLibJar(JobConf jobConf)
		{
			try
			{
				return jobConf.GetClassByName("testjar.ClassWordCount");
			}
			catch (TypeLoadException)
			{
				return null;
			}
		}
	}
}
