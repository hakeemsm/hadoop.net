using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A class that allows a map/red job to work on a sample of sequence files.
	/// 	</summary>
	/// <remarks>
	/// A class that allows a map/red job to work on a sample of sequence files.
	/// The sample is decided by the filter class set by the job.
	/// </remarks>
	public class SequenceFileInputFilter<K, V> : SequenceFileInputFormat<K, V>
	{
		private const string FilterClass = Org.Apache.Hadoop.Mapreduce.Lib.Input.SequenceFileInputFilter
			.FilterClass;

		public SequenceFileInputFilter()
		{
		}

		/// <summary>Create a record reader for the given split</summary>
		/// <param name="split">file split</param>
		/// <param name="job">job configuration</param>
		/// <param name="reporter">reporter who sends report to task tracker</param>
		/// <returns>RecordReader</returns>
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
			Reporter reporter)
		{
			reporter.SetStatus(split.ToString());
			return new SequenceFileInputFilter.FilterRecordReader<K, V>(job, (FileSplit)split
				);
		}

		/// <summary>set the filter class</summary>
		/// <param name="conf">application configuration</param>
		/// <param name="filterClass">filter class</param>
		public static void SetFilterClass(Configuration conf, Type filterClass)
		{
			conf.Set(FilterClass, filterClass.FullName);
		}

		/// <summary>filter interface</summary>
		public interface Filter : SequenceFileInputFilter.Filter
		{
		}

		/// <summary>base class for Filters</summary>
		public abstract class FilterBase : SequenceFileInputFilter.FilterBase, SequenceFileInputFilter.Filter
		{
			public abstract void SetConf(Configuration arg1);

			public abstract bool Accept(object arg1);
		}

		/// <summary>Records filter by matching key to regex</summary>
		public class RegexFilter : SequenceFileInputFilter.FilterBase
		{
			internal SequenceFileInputFilter.RegexFilter rf;

			/// <exception cref="Sharpen.PatternSyntaxException"/>
			public static void SetPattern(Configuration conf, string regex)
			{
				SequenceFileInputFilter.RegexFilter.SetPattern(conf, regex);
			}

			public RegexFilter()
			{
				rf = new SequenceFileInputFilter.RegexFilter();
			}

			/// <summary>configure the Filter by checking the configuration</summary>
			public override void SetConf(Configuration conf)
			{
				rf.SetConf(conf);
			}

			/// <summary>
			/// Filtering method
			/// If key matches the regex, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.SequenceFileInputFilter.Filter.Accept(object)
			/// 	"/>
			public override bool Accept(object key)
			{
				return rf.Accept(key);
			}
		}

		/// <summary>
		/// This class returns a percentage of records
		/// The percentage is determined by a filtering frequency <i>f</i> using
		/// the criteria record# % f == 0.
		/// </summary>
		/// <remarks>
		/// This class returns a percentage of records
		/// The percentage is determined by a filtering frequency <i>f</i> using
		/// the criteria record# % f == 0.
		/// For example, if the frequency is 10, one out of 10 records is returned.
		/// </remarks>
		public class PercentFilter : SequenceFileInputFilter.FilterBase
		{
			internal SequenceFileInputFilter.PercentFilter pf;

			/// <summary>set the frequency and stores it in conf</summary>
			/// <param name="conf">configuration</param>
			/// <param name="frequency">filtering frequencey</param>
			public static void SetFrequency(Configuration conf, int frequency)
			{
				SequenceFileInputFilter.PercentFilter.SetFrequency(conf, frequency);
			}

			public PercentFilter()
			{
				pf = new SequenceFileInputFilter.PercentFilter();
			}

			/// <summary>configure the filter by checking the configuration</summary>
			/// <param name="conf">configuration</param>
			public override void SetConf(Configuration conf)
			{
				pf.SetConf(conf);
			}

			/// <summary>
			/// Filtering method
			/// If record# % frequency==0, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.SequenceFileInputFilter.Filter.Accept(object)
			/// 	"/>
			public override bool Accept(object key)
			{
				return pf.Accept(key);
			}
		}

		/// <summary>
		/// This class returns a set of records by examing the MD5 digest of its
		/// key against a filtering frequency <i>f</i>.
		/// </summary>
		/// <remarks>
		/// This class returns a set of records by examing the MD5 digest of its
		/// key against a filtering frequency <i>f</i>. The filtering criteria is
		/// MD5(key) % f == 0.
		/// </remarks>
		public class MD5Filter : SequenceFileInputFilter.FilterBase
		{
			public const int Md5Len = SequenceFileInputFilter.MD5Filter.Md5Len;

			internal SequenceFileInputFilter.MD5Filter mf;

			/// <summary>set the filtering frequency in configuration</summary>
			/// <param name="conf">configuration</param>
			/// <param name="frequency">filtering frequency</param>
			public static void SetFrequency(Configuration conf, int frequency)
			{
				SequenceFileInputFilter.MD5Filter.SetFrequency(conf, frequency);
			}

			public MD5Filter()
			{
				mf = new SequenceFileInputFilter.MD5Filter();
			}

			/// <summary>configure the filter according to configuration</summary>
			/// <param name="conf">configuration</param>
			public override void SetConf(Configuration conf)
			{
				mf.SetConf(conf);
			}

			/// <summary>
			/// Filtering method
			/// If MD5(key) % frequency==0, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.SequenceFileInputFilter.Filter.Accept(object)
			/// 	"/>
			public override bool Accept(object key)
			{
				return mf.Accept(key);
			}
		}

		private class FilterRecordReader<K, V> : SequenceFileRecordReader<K, V>
		{
			private SequenceFileInputFilter.Filter filter;

			/// <exception cref="System.IO.IOException"/>
			public FilterRecordReader(Configuration conf, FileSplit split)
				: base(conf, split)
			{
				// instantiate filter
				filter = (SequenceFileInputFilter.Filter)ReflectionUtils.NewInstance(conf.GetClass
					(FilterClass, typeof(SequenceFileInputFilter.PercentFilter)), conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(K key, V value)
			{
				lock (this)
				{
					while (Next(key))
					{
						if (filter.Accept(key))
						{
							GetCurrentValue(value);
							return true;
						}
					}
					return false;
				}
			}
		}
	}
}
