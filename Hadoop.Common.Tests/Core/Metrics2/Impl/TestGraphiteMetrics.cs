using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Sink;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class TestGraphiteMetrics
	{
		private AbstractMetric MakeMetric(string name, Number value)
		{
			AbstractMetric metric = Org.Mockito.Mockito.Mock<AbstractMetric>();
			Org.Mockito.Mockito.When(metric.Name()).ThenReturn(name);
			Org.Mockito.Mockito.When(metric.Value()).ThenReturn(value);
			return metric;
		}

		private GraphiteSink.Graphite MakeGraphite()
		{
			GraphiteSink.Graphite mockGraphite = Org.Mockito.Mockito.Mock<GraphiteSink.Graphite
				>();
			Org.Mockito.Mockito.When(mockGraphite.IsConnected()).ThenReturn(true);
			return mockGraphite;
		}

		[Fact]
		public virtual void TestPutMetrics()
		{
			GraphiteSink sink = new GraphiteSink();
			IList<MetricsTag> tags = new AList<MetricsTag>();
			tags.AddItem(new MetricsTag(MsInfo.Context, "all"));
			tags.AddItem(new MetricsTag(MsInfo.Hostname, "host"));
			ICollection<AbstractMetric> metrics = new HashSet<AbstractMetric>();
			metrics.AddItem(MakeMetric("foo1", 1.25));
			metrics.AddItem(MakeMetric("foo2", 2.25));
			MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long)10000, tags, metrics
				);
			ArgumentCaptor<string> argument = ArgumentCaptor.ForClass<string>();
			GraphiteSink.Graphite mockGraphite = MakeGraphite();
			Whitebox.SetInternalState(sink, "graphite", mockGraphite);
			sink.PutMetrics(record);
			try
			{
				Org.Mockito.Mockito.Verify(mockGraphite).Write(argument.Capture());
			}
			catch (IOException e)
			{
				Runtime.PrintStackTrace(e);
			}
			string result = argument.GetValue();
			Assert.Equal(true, result.Equals("null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"
				 + "null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n") || result.Equals
				("null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n" + "null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"
				));
		}

		[Fact]
		public virtual void TestPutMetrics2()
		{
			GraphiteSink sink = new GraphiteSink();
			IList<MetricsTag> tags = new AList<MetricsTag>();
			tags.AddItem(new MetricsTag(MsInfo.Context, "all"));
			tags.AddItem(new MetricsTag(MsInfo.Hostname, null));
			ICollection<AbstractMetric> metrics = new HashSet<AbstractMetric>();
			metrics.AddItem(MakeMetric("foo1", 1));
			metrics.AddItem(MakeMetric("foo2", 2));
			MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long)10000, tags, metrics
				);
			ArgumentCaptor<string> argument = ArgumentCaptor.ForClass<string>();
			GraphiteSink.Graphite mockGraphite = MakeGraphite();
			Whitebox.SetInternalState(sink, "graphite", mockGraphite);
			sink.PutMetrics(record);
			try
			{
				Org.Mockito.Mockito.Verify(mockGraphite).Write(argument.Capture());
			}
			catch (IOException e)
			{
				Runtime.PrintStackTrace(e);
			}
			string result = argument.GetValue();
			Assert.Equal(true, result.Equals("null.all.Context.Context=all.foo1 1 10\n"
				 + "null.all.Context.Context=all.foo2 2 10\n") || result.Equals("null.all.Context.Context=all.foo2 2 10\n"
				 + "null.all.Context.Context=all.foo1 1 10\n"));
		}

		/// <summary>Assert that timestamps are converted correctly, ticket HADOOP-11182</summary>
		[Fact]
		public virtual void TestPutMetrics3()
		{
			// setup GraphiteSink
			GraphiteSink sink = new GraphiteSink();
			GraphiteSink.Graphite mockGraphite = MakeGraphite();
			Whitebox.SetInternalState(sink, "graphite", mockGraphite);
			// given two metrics records with timestamps 1000 milliseconds apart.
			IList<MetricsTag> tags = Collections.EmptyList();
			ICollection<AbstractMetric> metrics = new HashSet<AbstractMetric>();
			metrics.AddItem(MakeMetric("foo1", 1));
			MetricsRecord record1 = new MetricsRecordImpl(MsInfo.Context, 1000000000000L, tags
				, metrics);
			MetricsRecord record2 = new MetricsRecordImpl(MsInfo.Context, 1000000001000L, tags
				, metrics);
			sink.PutMetrics(record1);
			sink.PutMetrics(record2);
			sink.Flush();
			try
			{
				sink.Close();
			}
			catch (IOException e)
			{
				Runtime.PrintStackTrace(e);
			}
			// then the timestamps in the graphite stream should differ by one second.
			try
			{
				Org.Mockito.Mockito.Verify(mockGraphite).Write(Matchers.Eq("null.default.Context.foo1 1 1000000000\n"
					));
				Org.Mockito.Mockito.Verify(mockGraphite).Write(Matchers.Eq("null.default.Context.foo1 1 1000000001\n"
					));
			}
			catch (IOException e)
			{
				Runtime.PrintStackTrace(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFailureAndPutMetrics()
		{
			GraphiteSink sink = new GraphiteSink();
			IList<MetricsTag> tags = new AList<MetricsTag>();
			tags.AddItem(new MetricsTag(MsInfo.Context, "all"));
			tags.AddItem(new MetricsTag(MsInfo.Hostname, "host"));
			ICollection<AbstractMetric> metrics = new HashSet<AbstractMetric>();
			metrics.AddItem(MakeMetric("foo1", 1.25));
			metrics.AddItem(MakeMetric("foo2", 2.25));
			MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long)10000, tags, metrics
				);
			GraphiteSink.Graphite mockGraphite = MakeGraphite();
			Whitebox.SetInternalState(sink, "graphite", mockGraphite);
			// throw exception when first try
			Org.Mockito.Mockito.DoThrow(new IOException("IO exception")).When(mockGraphite).Write
				(Matchers.AnyString());
			sink.PutMetrics(record);
			Org.Mockito.Mockito.Verify(mockGraphite).Write(Matchers.AnyString());
			Org.Mockito.Mockito.Verify(mockGraphite).Close();
			// reset mock and try again
			Org.Mockito.Mockito.Reset(mockGraphite);
			Org.Mockito.Mockito.When(mockGraphite.IsConnected()).ThenReturn(false);
			ArgumentCaptor<string> argument = ArgumentCaptor.ForClass<string>();
			sink.PutMetrics(record);
			Org.Mockito.Mockito.Verify(mockGraphite).Write(argument.Capture());
			string result = argument.GetValue();
			Assert.Equal(true, result.Equals("null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"
				 + "null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n") || result.Equals
				("null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n" + "null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"
				));
		}

		[Fact]
		public virtual void TestClose()
		{
			GraphiteSink sink = new GraphiteSink();
			GraphiteSink.Graphite mockGraphite = MakeGraphite();
			Whitebox.SetInternalState(sink, "graphite", mockGraphite);
			try
			{
				sink.Close();
			}
			catch (IOException ioe)
			{
				Runtime.PrintStackTrace(ioe);
			}
			try
			{
				Org.Mockito.Mockito.Verify(mockGraphite).Close();
			}
			catch (IOException ioe)
			{
				Runtime.PrintStackTrace(ioe);
			}
		}
	}
}
