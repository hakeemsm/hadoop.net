using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	/// <summary>
	/// The constants that are going to be used by the timeline Kerberos + delegation
	/// token authentication.
	/// </summary>
	public class TimelineAuthenticationConsts
	{
		public const string ErrorExceptionJson = "exception";

		public const string ErrorClassnameJson = "javaClassName";

		public const string ErrorMessageJson = "message";

		public const string OpParam = "op";

		public const string DelegationParam = "delegation";

		public const string TokenParam = "token";

		public const string RenewerParam = "renewer";

		public const string DelegationTokenUrl = "url";

		public const string DelegationTokenExpirationTime = "expirationTime";
	}
}
