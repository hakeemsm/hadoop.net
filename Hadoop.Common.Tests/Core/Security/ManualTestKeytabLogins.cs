using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// Regression test for HADOOP-6947 which can be run manually in
	/// a kerberos environment.
	/// </summary>
	/// <remarks>
	/// Regression test for HADOOP-6947 which can be run manually in
	/// a kerberos environment.
	/// To run this test, set up two keytabs, each with a different principal.
	/// Then run something like:
	/// <code>
	/// HADOOP_CLASSPATH=build/test/classes bin/hadoop \
	/// org.apache.hadoop.security.ManualTestKeytabLogins \
	/// usera/test@REALM  /path/to/usera-keytab \
	/// userb/test@REALM  /path/to/userb-keytab
	/// </code>
	/// </remarks>
	public class ManualTestKeytabLogins
	{
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (args.Length != 4)
			{
				System.Console.Error.WriteLine("usage: ManualTestKeytabLogins <principal 1> <keytab 1> <principal 2> <keytab 2>"
					);
				System.Environment.Exit(1);
			}
			UserGroupInformation ugi1 = UserGroupInformation.LoginUserFromKeytabAndReturnUGI(
				args[0], args[1]);
			System.Console.Out.WriteLine("UGI 1 = " + ugi1);
			NUnit.Framework.Assert.IsTrue(ugi1.GetUserName().Equals(args[0]));
			UserGroupInformation ugi2 = UserGroupInformation.LoginUserFromKeytabAndReturnUGI(
				args[2], args[3]);
			System.Console.Out.WriteLine("UGI 2 = " + ugi2);
			NUnit.Framework.Assert.IsTrue(ugi2.GetUserName().Equals(args[2]));
		}
	}
}
