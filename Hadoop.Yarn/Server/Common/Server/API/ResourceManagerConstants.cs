using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	public abstract class ResourceManagerConstants
	{
		/// <summary>This states the invalid identifier of Resource Manager.</summary>
		/// <remarks>
		/// This states the invalid identifier of Resource Manager. This is used as a
		/// default value for initializing RM identifier. Currently, RM is using time
		/// stamp as RM identifier.
		/// </remarks>
		public const long RmInvalidIdentifier = -1;
	}

	public static class ResourceManagerConstantsConstants
	{
	}
}
