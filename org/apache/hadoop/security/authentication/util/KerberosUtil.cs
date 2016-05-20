using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class KerberosUtil
	{
		/* Return the Kerberos login module name */
		public static string getKrb5LoginModuleName()
		{
			return Sharpen.Runtime.getProperty("java.vendor").contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule"
				 : "com.sun.security.auth.module.Krb5LoginModule";
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="org.ietf.jgss.GSSException"/>
		/// <exception cref="java.lang.NoSuchFieldException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public static org.ietf.jgss.Oid getOidInstance(string oidName)
		{
			java.lang.Class oidClass;
			if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
			{
				if ("NT_GSS_KRB5_PRINCIPAL".Equals(oidName))
				{
					// IBM JDK GSSUtil class does not have field for krb5 principal oid
					return new org.ietf.jgss.Oid("1.2.840.113554.1.2.2.1");
				}
				oidClass = java.lang.Class.forName("com.ibm.security.jgss.GSSUtil");
			}
			else
			{
				oidClass = java.lang.Class.forName("sun.security.jgss.GSSUtil");
			}
			java.lang.reflect.Field oidField = oidClass.getDeclaredField(oidName);
			return (org.ietf.jgss.Oid)oidField.get(oidClass);
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="System.MissingMethodException"/>
		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		/// <exception cref="java.lang.reflect.InvocationTargetException"/>
		public static string getDefaultRealm()
		{
			object kerbConf;
			java.lang.Class classRef;
			java.lang.reflect.Method getInstanceMethod;
			java.lang.reflect.Method getDefaultRealmMethod;
			if (Sharpen.Runtime.getProperty("java.vendor").contains("IBM"))
			{
				classRef = java.lang.Class.forName("com.ibm.security.krb5.internal.Config");
			}
			else
			{
				classRef = java.lang.Class.forName("sun.security.krb5.Config");
			}
			getInstanceMethod = classRef.getMethod("getInstance", new java.lang.Class[0]);
			kerbConf = getInstanceMethod.invoke(classRef, new object[0]);
			getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new java.lang.Class
				[0]);
			return (string)getDefaultRealmMethod.invoke(kerbConf, new object[0]);
		}

		/* Return fqdn of the current host */
		/// <exception cref="java.net.UnknownHostException"/>
		internal static string getLocalHostName()
		{
			return java.net.InetAddress.getLocalHost().getCanonicalHostName();
		}

		/// <summary>Create Kerberos principal for a given service and hostname.</summary>
		/// <remarks>
		/// Create Kerberos principal for a given service and hostname. It converts
		/// hostname to lower case. If hostname is null or "0.0.0.0", it uses
		/// dynamically looked-up fqdn of the current host instead.
		/// </remarks>
		/// <param name="service">Service for which you want to generate the principal.</param>
		/// <param name="hostname">Fully-qualified domain name.</param>
		/// <returns>Converted Kerberos principal name.</returns>
		/// <exception cref="java.net.UnknownHostException">If no IP address for the local host could be found.
		/// 	</exception>
		public static string getServicePrincipal(string service, string hostname)
		{
			string fqdn = hostname;
			if (null == fqdn || fqdn.Equals(string.Empty) || fqdn.Equals("0.0.0.0"))
			{
				fqdn = getLocalHostName();
			}
			// convert hostname to lowercase as kerberos does not work with hostnames
			// with uppercase characters.
			return service + "/" + fqdn.ToLower(java.util.Locale.ENGLISH);
		}

		/// <summary>Get all the unique principals present in the keytabfile.</summary>
		/// <param name="keytabFileName">
		/// 
		/// Name of the keytab file to be read.
		/// </param>
		/// <returns>list of unique principals in the keytab.</returns>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If keytab entries cannot be read from the file.
		/// </exception>
		internal static string[] getPrincipalNames(string keytabFileName)
		{
			org.apache.directory.server.kerberos.shared.keytab.Keytab keytab = org.apache.directory.server.kerberos.shared.keytab.Keytab
				.read(new java.io.File(keytabFileName));
			System.Collections.Generic.ICollection<string> principals = new java.util.HashSet
				<string>();
			System.Collections.Generic.IList<org.apache.directory.server.kerberos.shared.keytab.KeytabEntry
				> entries = keytab.getEntries();
			foreach (org.apache.directory.server.kerberos.shared.keytab.KeytabEntry entry in 
				entries)
			{
				principals.add(entry.getPrincipalName().Replace("\\", "/"));
			}
			return Sharpen.Collections.ToArray(principals, new string[0]);
		}

		/// <summary>Get all the unique principals from keytabfile which matches a pattern.</summary>
		/// <param name="keytab">Name of the keytab file to be read.</param>
		/// <param name="pattern">pattern to be matched.</param>
		/// <returns>list of unique principals which matches the pattern.</returns>
		/// <exception cref="System.IO.IOException">if cannot get the principal name</exception>
		public static string[] getPrincipalNames(string keytab, java.util.regex.Pattern pattern
			)
		{
			string[] principals = getPrincipalNames(keytab);
			if (principals.Length != 0)
			{
				System.Collections.Generic.IList<string> matchingPrincipals = new System.Collections.Generic.List
					<string>();
				foreach (string principal in principals)
				{
					if (pattern.matcher(principal).matches())
					{
						matchingPrincipals.add(principal);
					}
				}
				principals = Sharpen.Collections.ToArray(matchingPrincipals, new string[0]);
			}
			return principals;
		}
	}
}
