using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using Org.Apache.Directory.Server.Kerberos.Shared.Keytab;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class KerberosUtil
	{
		/* Return the Kerberos login module name */
		public static string GetKrb5LoginModuleName()
		{
			return Runtime.GetProperty("java.vendor").Contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule"
				 : "com.sun.security.auth.module.Krb5LoginModule";
		}

		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="GSSException"/>
		/// <exception cref="NoSuchFieldException"/>
		/// <exception cref="System.MemberAccessException"/>
		public static Oid GetOidInstance(string oidName)
		{
			Type oidClass;
			if (PlatformName.IbmJava)
			{
				if ("NT_GSS_KRB5_PRINCIPAL".Equals(oidName))
				{
					// IBM JDK GSSUtil class does not have field for krb5 principal oid
					return new Oid("1.2.840.113554.1.2.2.1");
				}
				oidClass = Runtime.GetType("com.ibm.security.jgss.GSSUtil");
			}
			else
			{
				oidClass = Runtime.GetType("sun.security.jgss.GSSUtil");
			}
			FieldInfo oidField = Runtime.GetDeclaredField(oidClass, oidName);
			return (Oid)oidField.GetValue(oidClass);
		}

		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.MissingMethodException"/>
		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="System.MemberAccessException"/>
		/// <exception cref="System.Reflection.TargetInvocationException"/>
		public static string GetDefaultRealm()
		{
			object kerbConf;
			Type classRef;
			MethodInfo getInstanceMethod;
			MethodInfo getDefaultRealmMethod;
			if (Runtime.GetProperty("java.vendor").Contains("IBM"))
			{
				classRef = Runtime.GetType("com.ibm.security.krb5.internal.Config");
			}
			else
			{
				classRef = Runtime.GetType("sun.security.krb5.Config");
			}
			getInstanceMethod = classRef.GetMethod("getInstance", new Type[0]);
			kerbConf = getInstanceMethod.Invoke(classRef, new object[0]);
			getDefaultRealmMethod = Runtime.GetDeclaredMethod(classRef, "getDefaultRealm"
				, new Type[0]);
			return (string)getDefaultRealmMethod.Invoke(kerbConf, new object[0]);
		}

		/* Return fqdn of the current host */
		/// <exception cref="UnknownHostException"/>
		internal static string GetLocalHostName()
		{
			return Runtime.GetLocalHost().ToString();
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
		/// <exception cref="UnknownHostException">If no IP address for the local host could be found.
		/// 	</exception>
		public static string GetServicePrincipal(string service, string hostname)
		{
			string fqdn = hostname;
			if (null == fqdn || fqdn.Equals(string.Empty) || fqdn.Equals("0.0.0.0"))
			{
				fqdn = GetLocalHostName();
			}
			// convert hostname to lowercase as kerberos does not work with hostnames
			// with uppercase characters.
			return service + "/" + fqdn.ToLower(Extensions.GetEnglishCulture());
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
		internal static string[] GetPrincipalNames(string keytabFileName)
		{
			Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab keytab = Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab
				.Read(new FilePath(keytabFileName));
			ICollection<string> principals = new HashSet<string>();
			IList<KeytabEntry> entries = keytab.GetEntries();
			foreach (KeytabEntry entry in entries)
			{
				principals.AddItem(entry.GetPrincipalName().Replace("\\", "/"));
			}
			return Collections.ToArray(principals, new string[0]);
		}

		/// <summary>Get all the unique principals from keytabfile which matches a pattern.</summary>
		/// <param name="keytab">Name of the keytab file to be read.</param>
		/// <param name="pattern">pattern to be matched.</param>
		/// <returns>list of unique principals which matches the pattern.</returns>
		/// <exception cref="System.IO.IOException">if cannot get the principal name</exception>
		public static string[] GetPrincipalNames(string keytab, Pattern pattern)
		{
			string[] principals = GetPrincipalNames(keytab);
			if (principals.Length != 0)
			{
				IList<string> matchingPrincipals = new AList<string>();
				foreach (string principal in principals)
				{
					if (pattern.Matcher(principal).Matches())
					{
						matchingPrincipals.AddItem(principal);
					}
				}
				principals = Collections.ToArray(matchingPrincipals, new string[0]);
			}
			return principals;
		}
	}
}
