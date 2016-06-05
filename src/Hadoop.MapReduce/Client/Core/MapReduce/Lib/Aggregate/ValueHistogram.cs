using System;
using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that computes the
	/// histogram of a sequence of strings.
	/// </summary>
	public class ValueHistogram : ValueAggregator<string>
	{
		internal SortedDictionary<object, object> items = null;

		public ValueHistogram()
		{
			items = new SortedDictionary<object, object>();
		}

		/// <summary>add the given val to the aggregator.</summary>
		/// <param name="val">
		/// the value to be added. It is expected to be a string
		/// in the form of xxxx\tnum, meaning xxxx has num occurrences.
		/// </param>
		public virtual void AddNextValue(object val)
		{
			string valCountStr = val.ToString();
			int pos = valCountStr.LastIndexOf("\t");
			string valStr = valCountStr;
			string countStr = "1";
			if (pos >= 0)
			{
				valStr = Sharpen.Runtime.Substring(valCountStr, 0, pos);
				countStr = Sharpen.Runtime.Substring(valCountStr, pos + 1);
			}
			long count = (long)this.items[valStr];
			long inc = long.Parse(countStr);
			if (count == null)
			{
				count = inc;
			}
			else
			{
				count = count + inc;
			}
			items[valStr] = count;
		}

		/// <returns>
		/// the string representation of this aggregator.
		/// It includes the following basic statistics of the histogram:
		/// the number of unique values
		/// the minimum value
		/// the media value
		/// the maximum value
		/// the average value
		/// the standard deviation
		/// </returns>
		public virtual string GetReport()
		{
			long[] counts = new long[items.Count];
			StringBuilder sb = new StringBuilder();
			IEnumerator<object> iter = items.Values.GetEnumerator();
			int i = 0;
			while (iter.HasNext())
			{
				long count = (long)iter.Next();
				counts[i] = count;
				i += 1;
			}
			Arrays.Sort(counts);
			sb.Append(counts.Length);
			i = 0;
			long acc = 0;
			while (i < counts.Length)
			{
				long nextVal = counts[i];
				int j = i + 1;
				while (j < counts.Length && counts[j] == nextVal)
				{
					j++;
				}
				acc += nextVal * (j - i);
				i = j;
			}
			double average = 0.0;
			double sd = 0.0;
			if (counts.Length > 0)
			{
				sb.Append("\t").Append(counts[0]);
				sb.Append("\t").Append(counts[counts.Length / 2]);
				sb.Append("\t").Append(counts[counts.Length - 1]);
				average = acc * 1.0 / counts.Length;
				sb.Append("\t").Append(average);
				i = 0;
				while (i < counts.Length)
				{
					double nextDiff = counts[i] - average;
					sd += nextDiff * nextDiff;
					i += 1;
				}
				sd = Math.Sqrt(sd / counts.Length);
				sb.Append("\t").Append(sd);
			}
			return sb.ToString();
		}

		/// <returns>
		/// a string representation of the list of value/frequence pairs of
		/// the histogram
		/// </returns>
		public virtual string GetReportDetails()
		{
			StringBuilder sb = new StringBuilder();
			IEnumerator<KeyValuePair<object, object>> iter = items.GetEnumerator();
			while (iter.HasNext())
			{
				KeyValuePair<object, object> en = iter.Next();
				object val = en.Key;
				long count = (long)en.Value;
				sb.Append("\t").Append(val.ToString()).Append("\t").Append(count).Append("\n");
			}
			return sb.ToString();
		}

		/// <returns>
		/// a list value/frequence pairs.
		/// The return value is expected to be used by the reducer.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>();
			IEnumerator<KeyValuePair<object, object>> iter = items.GetEnumerator();
			while (iter.HasNext())
			{
				KeyValuePair<object, object> en = iter.Next();
				object val = en.Key;
				long count = (long)en.Value;
				retv.AddItem(val.ToString() + "\t" + count);
			}
			return retv;
		}

		/// <returns>a TreeMap representation of the histogram</returns>
		public virtual SortedDictionary<object, object> GetReportItems()
		{
			return items;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			items = new SortedDictionary<object, object>();
		}
	}
}
