using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>An object that allows you to set a limit on a stream.</summary>
	/// <remarks>
	/// An object that allows you to set a limit on a stream.  This limit
	/// represents the number of bytes that can be read without getting an
	/// exception.
	/// </remarks>
	internal interface StreamLimiter
	{
		/// <summary>Set a limit.</summary>
		/// <remarks>Set a limit.  Calling this function clears any existing limit.</remarks>
		void SetLimit(long limit);

		/// <summary>Disable limit.</summary>
		void ClearLimit();
	}
}
