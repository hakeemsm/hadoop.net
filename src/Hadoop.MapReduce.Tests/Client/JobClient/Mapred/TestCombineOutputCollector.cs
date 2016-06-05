using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestCombineOutputCollector
	{
		private Task.CombineOutputCollector<string, int> coc;

		private sealed class _Counter_40 : Counters.Counter
		{
			public _Counter_40()
			{
			}

			private long value;

			public override void SetValue(long value)
			{
				this.value = value;
			}

			public override void SetDisplayName(string displayName)
			{
			}

			// TODO Auto-generated method stub
			public override void Increment(long incr)
			{
				this.value += incr;
			}

			public override long GetValue()
			{
				return this.value;
			}

			public override string GetName()
			{
				// TODO Auto-generated method stub
				return null;
			}

			public override string GetDisplayName()
			{
				// TODO Auto-generated method stub
				return null;
			}

			public override string MakeEscapedCompactString()
			{
				// TODO Auto-generated method stub
				return null;
			}

			public override long GetCounter()
			{
				return this.value;
			}

			public override bool ContentEquals(Counters.Counter counter)
			{
				// TODO Auto-generated method stub
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(DataInput @in)
			{
			}
		}

		internal Counters.Counter outCounter = new _Counter_40();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomCollect()
		{
			//mock creation
			Task.TaskReporter mockTaskReporter = Org.Mockito.Mockito.Mock<Task.TaskReporter>(
				);
			IFile.Writer<string, int> mockWriter = Org.Mockito.Mockito.Mock<IFile.Writer>();
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.CombineRecordsBeforeProgress, "2");
			coc = new Task.CombineOutputCollector<string, int>(outCounter, mockTaskReporter, 
				conf);
			coc.SetWriter(mockWriter);
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Never()).Progress
				();
			coc.Collect("dummy", 1);
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Never()).Progress
				();
			coc.Collect("dummy", 2);
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Times(1)).Progress
				();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultCollect()
		{
			//mock creation
			Task.TaskReporter mockTaskReporter = Org.Mockito.Mockito.Mock<Task.TaskReporter>(
				);
			IFile.Writer<string, int> mockWriter = Org.Mockito.Mockito.Mock<IFile.Writer>();
			Configuration conf = new Configuration();
			coc = new Task.CombineOutputCollector<string, int>(outCounter, mockTaskReporter, 
				conf);
			coc.SetWriter(mockWriter);
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Never()).Progress
				();
			for (int i = 0; i < Task.DefaultCombineRecordsBeforeProgress; i++)
			{
				coc.Collect("dummy", i);
			}
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Times(1)).Progress
				();
			for (int i_1 = 0; i_1 < Task.DefaultCombineRecordsBeforeProgress; i_1++)
			{
				coc.Collect("dummy", i_1);
			}
			Org.Mockito.Mockito.Verify(mockTaskReporter, Org.Mockito.Mockito.Times(2)).Progress
				();
		}
	}
}
