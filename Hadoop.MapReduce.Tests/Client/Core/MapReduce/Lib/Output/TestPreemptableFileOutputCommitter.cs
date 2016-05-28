using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestPreemptableFileOutputCommitter
	{
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPartialOutputCleanup()
		{
			Configuration conf = new Configuration(false);
			conf.SetInt(MRJobConfig.ApplicationAttemptId, 1);
			TaskAttemptID tid0 = new TaskAttemptID("1363718006656", 1, TaskType.Reduce, 14, 3
				);
			Path p = Org.Mockito.Mockito.Spy(new Path("/user/hadoop/out"));
			Path a = new Path("hdfs://user/hadoop/out");
			Path p0 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_0");
			Path p1 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_1");
			Path p2 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000013_0");
			// (p3 does not exist)
			Path p3 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_2");
			FileStatus[] fsa = new FileStatus[3];
			fsa[0] = new FileStatus();
			fsa[0].SetPath(p0);
			fsa[1] = new FileStatus();
			fsa[1].SetPath(p1);
			fsa[2] = new FileStatus();
			fsa[2].SetPath(p2);
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Exists(Eq(p0))).ThenReturn(true);
			Org.Mockito.Mockito.When(fs.Exists(Eq(p1))).ThenReturn(true);
			Org.Mockito.Mockito.When(fs.Exists(Eq(p2))).ThenReturn(true);
			Org.Mockito.Mockito.When(fs.Exists(Eq(p3))).ThenReturn(false);
			Org.Mockito.Mockito.When(fs.Delete(Eq(p0), Eq(true))).ThenReturn(true);
			Org.Mockito.Mockito.When(fs.Delete(Eq(p1), Eq(true))).ThenReturn(true);
			Org.Mockito.Mockito.DoReturn(fs).When(p).GetFileSystem(Any<Configuration>());
			Org.Mockito.Mockito.When(fs.MakeQualified(Eq(p))).ThenReturn(a);
			TaskAttemptContext context = Org.Mockito.Mockito.Mock<TaskAttemptContext>();
			Org.Mockito.Mockito.When(context.GetTaskAttemptID()).ThenReturn(tid0);
			Org.Mockito.Mockito.When(context.GetConfiguration()).ThenReturn(conf);
			PartialFileOutputCommitter foc = new TestPreemptableFileOutputCommitter.TestPFOC(
				p, context, fs);
			foc.CleanUpPartialOutputForTask(context);
			Org.Mockito.Mockito.Verify(fs).Delete(Eq(p0), Eq(true));
			Org.Mockito.Mockito.Verify(fs).Delete(Eq(p1), Eq(true));
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).Delete(Eq(p3), Eq(true
				));
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).Delete(Eq(p2), Eq(true
				));
		}

		internal class TestPFOC : PartialFileOutputCommitter
		{
			internal readonly FileSystem fs;

			/// <exception cref="System.IO.IOException"/>
			internal TestPFOC(Path outputPath, TaskAttemptContext ctxt, FileSystem fs)
				: base(outputPath, ctxt)
			{
				this.fs = fs;
			}

			internal override FileSystem FsFor(Path p, Configuration conf)
			{
				return fs;
			}
		}
	}
}
