using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary>An interface for reporting exceptions to other threads</summary>
	public interface ExceptionReporter
	{
		void ReportException(Exception t);
	}
}
