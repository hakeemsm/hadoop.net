using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJobClientGetJob
	{
		private static Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", 
			"/tmp"));

		/// <exception cref="System.IO.IOException"/>
		private Path CreateTempFile(string filename, string contents)
		{
			Path path = new Path(TestRootDir, filename);
			Configuration conf = new Configuration();
			FSDataOutputStream os = FileSystem.GetLocal(conf).Create(path);
			os.WriteBytes(contents);
			os.Close();
			return path;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRunningJobFromJobClient()
		{
			JobConf conf = new JobConf();
			conf.Set("mapreduce.framework.name", "local");
			FileInputFormat.AddInputPath(conf, CreateTempFile("in", "hello"));
			Path outputDir = new Path(TestRootDir, GetType().Name);
			outputDir.GetFileSystem(conf).Delete(outputDir, true);
			FileOutputFormat.SetOutputPath(conf, outputDir);
			JobClient jc = new JobClient(conf);
			RunningJob runningJob = jc.SubmitJob(conf);
			NUnit.Framework.Assert.IsNotNull("Running job", runningJob);
			// Check that the running job can be retrieved by ID
			RunningJob newRunningJob = jc.GetJob(runningJob.GetID());
			NUnit.Framework.Assert.IsNotNull("New running job", newRunningJob);
		}
	}
}
