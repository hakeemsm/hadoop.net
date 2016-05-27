using System.Collections.Generic;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Protocol which is used to refresh arbitrary things at runtime.</summary>
	public abstract class GenericRefreshProtocol
	{
		/// <summary>Version 1: Initial version.</summary>
		public const long versionID = 1L;

		/// <summary>Refresh the resource based on identity passed in.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract ICollection<RefreshResponse> Refresh(string identifier, string[] 
			args);
	}

	public static class GenericRefreshProtocolConstants
	{
	}
}
