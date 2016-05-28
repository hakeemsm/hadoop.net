using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>
	/// Exception thrown when too many exceptions occur while gathering
	/// responses to a quorum call.
	/// </summary>
	[System.Serializable]
	internal class QuorumException : IOException
	{
		/// <summary>
		/// Create a QuorumException instance with a descriptive message detailing
		/// the underlying exceptions, as well as any successful responses which
		/// were returned.
		/// </summary>
		/// <?/>
		/// <?/>
		/// <param name="successes">any successful responses returned</param>
		/// <param name="exceptions">the exceptions returned</param>
		public static Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumException Create<K, V>
			(string simpleMsg, IDictionary<K, V> successes, IDictionary<K, Exception> exceptions
			)
		{
			Preconditions.CheckArgument(!exceptions.IsEmpty(), "Must pass exceptions");
			StringBuilder msg = new StringBuilder();
			msg.Append(simpleMsg).Append(". ");
			if (!successes.IsEmpty())
			{
				msg.Append(successes.Count).Append(" successful responses:\n");
				Joiner.On("\n").UseForNull("null [success]").WithKeyValueSeparator(": ").AppendTo
					(msg, successes);
				msg.Append("\n");
			}
			msg.Append(exceptions.Count + " exceptions thrown:\n");
			bool isFirst = true;
			foreach (KeyValuePair<K, Exception> e in exceptions)
			{
				if (!isFirst)
				{
					msg.Append("\n");
				}
				isFirst = false;
				msg.Append(e.Key).Append(": ");
				if (e.Value is RuntimeException)
				{
					msg.Append(StringUtils.StringifyException(e.Value));
				}
				else
				{
					if (e.Value.GetLocalizedMessage() != null)
					{
						msg.Append(e.Value.GetLocalizedMessage());
					}
					else
					{
						msg.Append(StringUtils.StringifyException(e.Value));
					}
				}
			}
			return new Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumException(msg.ToString());
		}

		private QuorumException(string msg)
			: base(msg)
		{
		}

		private const long serialVersionUID = 1L;
	}
}
