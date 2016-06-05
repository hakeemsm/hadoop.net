using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over date/time values returned by an Oracle db.</summary>
	/// <remarks>
	/// Implement DBSplitter over date/time values returned by an Oracle db.
	/// Make use of logic from DateSplitter, since this just needs to use
	/// some Oracle-specific functions on the formatting end when generating
	/// InputSplits.
	/// </remarks>
	public class OracleDateSplitter : DateSplitter
	{
		protected internal override string DateToString(DateTime d)
		{
			// Oracle Data objects are always actually Timestamps
			return "TO_TIMESTAMP('" + d.ToString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
		}
	}
}
