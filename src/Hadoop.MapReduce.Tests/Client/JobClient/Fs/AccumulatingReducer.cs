using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Reducer that accumulates values based on their type.</summary>
	/// <remarks>
	/// Reducer that accumulates values based on their type.
	/// <p>
	/// The type is specified in the key part of the key-value pair
	/// as a prefix to the key in the following way
	/// <p>
	/// <tt>type:key</tt>
	/// <p>
	/// The values are accumulated according to the types:
	/// <ul>
	/// <li><tt>s:</tt> - string, concatenate</li>
	/// <li><tt>f:</tt> - float, summ</li>
	/// <li><tt>l:</tt> - long, summ</li>
	/// </ul>
	/// </remarks>
	public class AccumulatingReducer : MapReduceBase, Reducer<Text, Text, Text, Text>
	{
		internal const string ValueTypeLong = "l:";

		internal const string ValueTypeFloat = "f:";

		internal const string ValueTypeString = "s:";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.AccumulatingReducer
			));

		protected internal string hostName;

		public AccumulatingReducer()
		{
			try
			{
				hostName = Sharpen.Runtime.GetLocalHost().GetHostName();
			}
			catch (Exception)
			{
				hostName = "localhost";
			}
			Log.Info("Starting AccumulatingReducer on " + hostName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
			, Text> output, Reporter reporter)
		{
			string field = key.ToString();
			reporter.SetStatus("starting " + field + " ::host = " + hostName);
			// concatenate strings
			if (field.StartsWith(ValueTypeString))
			{
				StringBuilder sSum = new StringBuilder();
				while (values.HasNext())
				{
					sSum.Append(values.Next().ToString()).Append(";");
				}
				output.Collect(key, new Org.Apache.Hadoop.IO.Text(sSum.ToString()));
				reporter.SetStatus("finished " + field + " ::host = " + hostName);
				return;
			}
			// sum long values
			if (field.StartsWith(ValueTypeFloat))
			{
				float fSum = 0;
				while (values.HasNext())
				{
					fSum += float.ParseFloat(values.Next().ToString());
				}
				output.Collect(key, new Org.Apache.Hadoop.IO.Text(fSum.ToString()));
				reporter.SetStatus("finished " + field + " ::host = " + hostName);
				return;
			}
			// sum long values
			if (field.StartsWith(ValueTypeLong))
			{
				long lSum = 0;
				while (values.HasNext())
				{
					lSum += long.Parse(values.Next().ToString());
				}
				output.Collect(key, new Org.Apache.Hadoop.IO.Text(lSum.ToString()));
			}
			reporter.SetStatus("finished " + field + " ::host = " + hostName);
		}
	}
}
