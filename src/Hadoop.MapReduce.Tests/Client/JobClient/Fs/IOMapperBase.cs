using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Base mapper class for IO operations.</summary>
	/// <remarks>
	/// Base mapper class for IO operations.
	/// <p>
	/// Two abstract method
	/// <see cref="IOMapperBase{T}.DoIO(Org.Apache.Hadoop.Mapred.Reporter, string, long)"
	/// 	/>
	/// and
	/// <see cref="IOMapperBase{T}.CollectStats(Org.Apache.Hadoop.Mapred.OutputCollector{K, V}, string, long, object)
	/// 	"/>
	/// should be
	/// overloaded in derived classes to define the IO operation and the
	/// statistics data to be collected by subsequent reducers.
	/// </remarks>
	public abstract class IOMapperBase<T> : Configured, Mapper<Text, LongWritable, Text
		, Text>
	{
		protected internal byte[] buffer;

		protected internal int bufferSize;

		protected internal FileSystem fs;

		protected internal string hostName;

		protected internal IDisposable stream;

		public IOMapperBase()
		{
		}

		public virtual void Configure(JobConf conf)
		{
			SetConf(conf);
			try
			{
				fs = FileSystem.Get(conf);
			}
			catch (Exception e)
			{
				throw new RuntimeException("Cannot create file system.", e);
			}
			bufferSize = conf.GetInt("test.io.file.buffer.size", 4096);
			buffer = new byte[bufferSize];
			try
			{
				hostName = Sharpen.Runtime.GetLocalHost().GetHostName();
			}
			catch (Exception)
			{
				hostName = "localhost";
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		/// <summary>Perform io operation, usually read or write.</summary>
		/// <param name="reporter"/>
		/// <param name="name">file name</param>
		/// <param name="value">offset within the file</param>
		/// <returns>
		/// object that is passed as a parameter to
		/// <see cref="IOMapperBase{T}.CollectStats(Org.Apache.Hadoop.Mapred.OutputCollector{K, V}, string, long, object)
		/// 	"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal abstract T DoIO(Reporter reporter, string name, long value);

		/// <summary>Create an input or output stream based on the specified file.</summary>
		/// <remarks>
		/// Create an input or output stream based on the specified file.
		/// Subclasses should override this method to provide an actual stream.
		/// </remarks>
		/// <param name="name">file name</param>
		/// <returns>the stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IDisposable GetIOStream(string name)
		{
			return null;
		}

		/// <summary>Collect stat data to be combined by a subsequent reducer.</summary>
		/// <param name="output"/>
		/// <param name="name">file name</param>
		/// <param name="execTime">IO execution time</param>
		/// <param name="doIOReturnValue">
		/// value returned by
		/// <see cref="IOMapperBase{T}.DoIO(Org.Apache.Hadoop.Mapred.Reporter, string, long)"
		/// 	/>
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void CollectStats(OutputCollector<Text, Text> output, string name
			, long execTime, T doIOReturnValue);

		/// <summary>Map file name and offset into statistical data.</summary>
		/// <remarks>
		/// Map file name and offset into statistical data.
		/// <p>
		/// The map task is to get the
		/// <tt>key</tt>, which contains the file name, and the
		/// <tt>value</tt>, which is the offset within the file.
		/// The parameters are passed to the abstract method
		/// <see cref="IOMapperBase{T}.DoIO(Org.Apache.Hadoop.Mapred.Reporter, string, long)"
		/// 	/>
		/// , which performs the io operation,
		/// usually read or write data, and then
		/// <see cref="IOMapperBase{T}.CollectStats(Org.Apache.Hadoop.Mapred.OutputCollector{K, V}, string, long, object)
		/// 	"/>
		/// 
		/// is called to prepare stat data for a subsequent reducer.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(Text key, LongWritable value, OutputCollector<Text, Text>
			 output, Reporter reporter)
		{
			string name = key.ToString();
			long longValue = value.Get();
			reporter.SetStatus("starting " + name + " ::host = " + hostName);
			this.stream = GetIOStream(name);
			T statValue = null;
			long tStart = Runtime.CurrentTimeMillis();
			try
			{
				statValue = DoIO(reporter, name, longValue);
			}
			finally
			{
				if (stream != null)
				{
					stream.Close();
				}
			}
			long tEnd = Runtime.CurrentTimeMillis();
			long execTime = tEnd - tStart;
			CollectStats(output, name, execTime, statValue);
			reporter.SetStatus("finished " + name + " ::host = " + hostName);
		}
	}
}
