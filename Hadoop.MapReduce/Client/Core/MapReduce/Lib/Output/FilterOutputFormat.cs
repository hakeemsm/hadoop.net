using System.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>FilterOutputFormat is a convenience class that wraps OutputFormat.</summary>
	public class FilterOutputFormat<K, V> : OutputFormat<K, V>
	{
		protected internal OutputFormat<K, V> baseOut;

		public FilterOutputFormat()
		{
			this.baseOut = null;
		}

		/// <summary>Create a FilterOutputFormat based on the underlying output format.</summary>
		/// <param name="baseOut">the underlying OutputFormat</param>
		public FilterOutputFormat(OutputFormat<K, V> baseOut)
		{
			this.baseOut = baseOut;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context)
		{
			return GetBaseOut().GetRecordWriter(context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CheckOutputSpecs(JobContext context)
		{
			GetBaseOut().CheckOutputSpecs(context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			return GetBaseOut().GetOutputCommitter(context);
		}

		/// <exception cref="System.IO.IOException"/>
		private OutputFormat<K, V> GetBaseOut()
		{
			if (baseOut == null)
			{
				throw new IOException("OutputFormat not set for FilterOutputFormat");
			}
			return baseOut;
		}

		/// <summary>
		/// <code>FilterRecordWriter</code> is a convenience wrapper
		/// class that extends the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.RecordWriter{K, V}"/>
		/// .
		/// </summary>
		public class FilterRecordWriter<K, V> : RecordWriter<K, V>
		{
			protected internal RecordWriter<K, V> rawWriter = null;

			public FilterRecordWriter()
			{
				rawWriter = null;
			}

			public FilterRecordWriter(RecordWriter<K, V> rwriter)
			{
				this.rawWriter = rwriter;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K key, V value)
			{
				GetRawWriter().Write(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				GetRawWriter().Close(context);
			}

			/// <exception cref="System.IO.IOException"/>
			private RecordWriter<K, V> GetRawWriter()
			{
				if (rawWriter == null)
				{
					throw new IOException("Record Writer not set for FilterRecordWriter");
				}
				return rawWriter;
			}
		}
	}
}
