

namespace Org.Apache.Hadoop.Security
{
	/// <summary>helper utils for tests</summary>
	public class SecurityUtilTestHelper
	{
		/// <summary>Allow tests to change the resolver used for tokens</summary>
		/// <param name="flag">boolean for whether token services use ips or hosts</param>
		public static void SetTokenServiceUseIp(bool flag)
		{
			SecurityUtil.SetTokenServiceUseIp(flag);
		}

		/// <summary>
		/// Return true if externalKdc=true and the location of the krb5.conf
		/// file has been specified, and false otherwise.
		/// </summary>
		public static bool IsExternalKdcRunning()
		{
			string externalKdc = Runtime.GetProperty("externalKdc");
			string krb5Conf = Runtime.GetProperty("java.security.krb5.conf");
			if (externalKdc == null || !externalKdc.Equals("true") || krb5Conf == null)
			{
				return false;
			}
			return true;
		}
	}
}
