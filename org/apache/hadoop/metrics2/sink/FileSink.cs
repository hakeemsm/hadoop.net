using Sharpen;

namespace org.apache.hadoop.metrics2.sink
{
	/// <summary>A metrics sink that writes to a file</summary>
	public class FileSink : org.apache.hadoop.metrics2.MetricsSink, java.io.Closeable
	{
		private const string FILENAME_KEY = "filename";

		private System.IO.TextWriter writer;

		public virtual void init(org.apache.commons.configuration.SubsetConfiguration conf
			)
		{
			string filename = conf.getString(FILENAME_KEY);
			try
			{
				writer = filename == null ? System.Console.Out : new System.IO.TextWriter(new java.io.FileOutputStream
					(new java.io.File(filename)), true, "UTF-8");
			}
			catch (System.Exception e)
			{
				throw new org.apache.hadoop.metrics2.MetricsException("Error creating " + filename
					, e);
			}
		}

		public virtual void putMetrics(org.apache.hadoop.metrics2.MetricsRecord record)
		{
			writer.Write(record.timestamp());
			writer.Write(" ");
			writer.Write(record.context());
			writer.Write(".");
			writer.Write(record.name());
			string separator = ": ";
			foreach (org.apache.hadoop.metrics2.MetricsTag tag in record.tags())
			{
				writer.Write(separator);
				separator = ", ";
				writer.Write(tag.name());
				writer.Write("=");
				writer.Write(tag.value());
			}
			foreach (org.apache.hadoop.metrics2.AbstractMetric metric in record.metrics())
			{
				writer.Write(separator);
				separator = ", ";
				writer.Write(metric.name());
				writer.Write("=");
				writer.Write(metric.value());
			}
			writer.WriteLine();
		}

		public virtual void flush()
		{
			writer.flush();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			writer.close();
		}
	}
}
