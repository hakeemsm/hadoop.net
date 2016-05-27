using System;
using System.IO;
using Org.Apache.Commons.Configuration;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink
{
	/// <summary>A metrics sink that writes to a file</summary>
	public class FileSink : MetricsSink, IDisposable
	{
		private const string FilenameKey = "filename";

		private TextWriter writer;

		public virtual void Init(SubsetConfiguration conf)
		{
			string filename = conf.GetString(FilenameKey);
			try
			{
				writer = filename == null ? System.Console.Out : new TextWriter(new FileOutputStream
					(new FilePath(filename)), true, "UTF-8");
			}
			catch (Exception e)
			{
				throw new MetricsException("Error creating " + filename, e);
			}
		}

		public virtual void PutMetrics(MetricsRecord record)
		{
			writer.Write(record.Timestamp());
			writer.Write(" ");
			writer.Write(record.Context());
			writer.Write(".");
			writer.Write(record.Name());
			string separator = ": ";
			foreach (MetricsTag tag in record.Tags())
			{
				writer.Write(separator);
				separator = ", ";
				writer.Write(tag.Name());
				writer.Write("=");
				writer.Write(tag.Value());
			}
			foreach (AbstractMetric metric in record.Metrics())
			{
				writer.Write(separator);
				separator = ", ";
				writer.Write(metric.Name());
				writer.Write("=");
				writer.Write(metric.Value());
			}
			writer.WriteLine();
		}

		public virtual void Flush()
		{
			writer.Flush();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			writer.Close();
		}
	}
}
