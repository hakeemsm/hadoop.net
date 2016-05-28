using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>Abstract base of internal data structures used for tracking progress.</summary>
	/// <remarks>
	/// Abstract base of internal data structures used for tracking progress.  For
	/// primitive long properties,
	/// <see cref="long.MinValue"/>
	/// is used as a sentinel value
	/// to indicate that the property is undefined.
	/// </remarks>
	internal abstract class AbstractTracking : ICloneable
	{
		internal long beginTime = long.MinValue;

		internal long endTime = long.MinValue;

		/// <summary>
		/// Subclass instances may call this method during cloning to copy the values of
		/// all properties stored in this base class.
		/// </summary>
		/// <param name="dest">AbstractTracking destination for copying properties</param>
		protected internal virtual void Copy(AbstractTracking dest)
		{
			dest.beginTime = beginTime;
			dest.endTime = endTime;
		}
	}
}
