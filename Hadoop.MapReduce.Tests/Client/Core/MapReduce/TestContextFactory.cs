using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestContextFactory
	{
		internal JobID jobId;

		internal Configuration conf;

		internal JobContext jobContext;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			jobId = new JobID("test", 1);
			jobContext = new JobContextImpl(conf, jobId);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCloneContext()
		{
			ContextFactory.CloneContext(jobContext, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCloneMapContext()
		{
			TaskID taskId = new TaskID(jobId, TaskType.Map, 0);
			TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);
			MapContext<IntWritable, IntWritable, IntWritable, IntWritable> mapContext = new MapContextImpl
				<IntWritable, IntWritable, IntWritable, IntWritable>(conf, taskAttemptid, null, 
				null, null, null, null);
			Mapper.Context mapperContext = new WrappedMapper<IntWritable, IntWritable, IntWritable
				, IntWritable>().GetMapContext(mapContext);
			ContextFactory.CloneMapContext(mapperContext, conf, null, null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void TearDown()
		{
		}
	}
}
