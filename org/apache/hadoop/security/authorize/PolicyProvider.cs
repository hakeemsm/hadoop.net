using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>
	/// <see cref="PolicyProvider"/>
	/// provides the
	/// <see cref="Service"/>
	/// definitions to the
	/// security
	/// <see cref="java.security.Policy"/>
	/// in effect for Hadoop.
	/// </summary>
	public abstract class PolicyProvider
	{
		/// <summary>
		/// Configuration key for the
		/// <see cref="PolicyProvider"/>
		/// implementation.
		/// </summary>
		public const string POLICY_PROVIDER_CONFIG = "hadoop.security.authorization.policyprovider";

		private sealed class _PolicyProvider_44 : org.apache.hadoop.security.authorize.PolicyProvider
		{
			public _PolicyProvider_44()
			{
			}

			public override org.apache.hadoop.security.authorize.Service[] getServices()
			{
				return null;
			}
		}

		/// <summary>
		/// A default
		/// <see cref="PolicyProvider"/>
		/// without any defined services.
		/// </summary>
		public static readonly org.apache.hadoop.security.authorize.PolicyProvider DEFAULT_POLICY_PROVIDER
			 = new _PolicyProvider_44();

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
		public abstract org.apache.hadoop.security.authorize.Service[] getServices();
	}
}
