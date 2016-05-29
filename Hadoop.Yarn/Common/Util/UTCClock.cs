using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// Implementation of
	/// <see cref="Clock"/>
	/// that gives the current UTC time in
	/// milliseconds.
	/// </summary>
	public class UTCClock : Clock
	{
		private readonly TimeZoneInfo utcZone = Sharpen.Extensions.GetTimeZone("UTC");

		public virtual long GetTime()
		{
			return Calendar.GetInstance(utcZone).GetTimeInMillis();
		}
	}
}
