using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	/// <summary>
	/// <see cref="PolicyProvider"/>
	/// provides the
	/// <see cref="Service"/>
	/// definitions to the
	/// security
	/// <see cref="Sharpen.Policy"/>
	/// in effect for Hadoop.
	/// </summary>
	public abstract class PolicyProvider
	{
		/// <summary>
		/// Configuration key for the
		/// <see cref="PolicyProvider"/>
		/// implementation.
		/// </summary>
		public const string PolicyProviderConfig = "hadoop.security.authorization.policyprovider";

		private sealed class _PolicyProvider_44 : PolicyProvider
		{
			public _PolicyProvider_44()
			{
			}

			public override Service[] GetServices()
			{
				return null;
			}
		}

		/// <summary>
		/// A default
		/// <see cref="PolicyProvider"/>
		/// without any defined services.
		/// </summary>
		public static readonly PolicyProvider DefaultPolicyProvider = new _PolicyProvider_44
			();

		/// <summary>
		/// Get the
		/// <see cref="Service"/>
		/// definitions from the
		/// <see cref="PolicyProvider"/>
		/// .
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Service"/>
		/// definitions
		/// </returns>
		public abstract Service[] GetServices();
	}
}
