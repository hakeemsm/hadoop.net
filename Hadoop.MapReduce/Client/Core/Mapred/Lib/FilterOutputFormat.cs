using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>FilterOutputFormat is a convenience class that wraps OutputFormat.</summary>
	public class FilterOutputFormat<K, V> : OutputFormat<K, V>
	{
		protected internal OutputFormat<K, V> baseOut;

		public FilterOutputFormat()
		{
			this.baseOut = null;
		}

		/// <summary>Create a FilterOutputFormat based on the supplied output format.</summary>
		/// <param name="out">the underlying OutputFormat</param>
		public FilterOutputFormat(OutputFormat<K, V> @out)
		{
			this.baseOut = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
			, string name, Progressable progress)
		{
			return GetBaseOut().GetRecordWriter(ignored, job, name, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckOutputSpecs(FileSystem ignored, JobConf job)
		{
			GetBaseOut().CheckOutputSpecs(ignored, job);
		}

		/// <exception cref="System.IO.IOException"/>
		private OutputFormat<K, V> GetBaseOut()
		{
			if (baseOut == null)
			{
				throw new IOException("Outputformat not set for FilterOutputFormat");
			}
			return baseOut;
		}

		/// <summary>
		/// <code>FilterRecordWriter</code> is a convenience wrapper
		/// class that implements
		/// <see cref="Org.Apache.Hadoop.Mapred.RecordWriter{K, V}"/>
		/// .
		/// </summary>
		public class FilterRecordWriter<K, V> : RecordWriter<K, V>
		{
			protected internal RecordWriter<K, V> rawWriter = null;

			/// <exception cref="System.IO.IOException"/>
			public FilterRecordWriter()
			{
				rawWriter = null;
			}

			/// <exception cref="System.IO.IOException"/>
			public FilterRecordWriter(RecordWriter<K, V> rawWriter)
			{
				this.rawWriter = rawWriter;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close(Reporter reporter)
			{
				GetRawWriter().Close(reporter);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(K key, V value)
			{
				GetRawWriter().Write(key, value);
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
