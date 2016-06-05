using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang.Text;
using Org.Apache.Directory.Api.Ldap.Model.Entry;
using Org.Apache.Directory.Api.Ldap.Model.Ldif;
using Org.Apache.Directory.Api.Ldap.Model.Name;
using Org.Apache.Directory.Api.Ldap.Model.Schema;
using Org.Apache.Directory.Api.Ldap.Model.Schema.Registries;
using Org.Apache.Directory.Api.Ldap.Schemaextractor;
using Org.Apache.Directory.Api.Ldap.Schemaextractor.Impl;
using Org.Apache.Directory.Api.Ldap.Schemaloader;
using Org.Apache.Directory.Api.Ldap.Schemamanager.Impl;
using Org.Apache.Directory.Server.Constants;
using Org.Apache.Directory.Server.Core;
using Org.Apache.Directory.Server.Core.Api;
using Org.Apache.Directory.Server.Core.Api.Schema;
using Org.Apache.Directory.Server.Core.Kerberos;
using Org.Apache.Directory.Server.Core.Partition.Impl.Btree.Jdbm;
using Org.Apache.Directory.Server.Core.Partition.Ldif;
using Org.Apache.Directory.Server.Kerberos;
using Org.Apache.Directory.Server.Kerberos.Kdc;
using Org.Apache.Directory.Server.Kerberos.Shared.Crypto.Encryption;
using Org.Apache.Directory.Server.Kerberos.Shared.Keytab;
using Org.Apache.Directory.Server.Protocol.Shared.Transport;
using Org.Apache.Directory.Server.Xdbm;
using Org.Apache.Directory.Shared.Kerberos;
using Org.Apache.Directory.Shared.Kerberos.Codec.Types;
using Org.Apache.Directory.Shared.Kerberos.Components;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Minikdc
{
	/// <summary>
	/// Mini KDC based on Apache Directory Server that can be embedded in testcases
	/// or used from command line as a standalone KDC.
	/// </summary>
	/// <remarks>
	/// Mini KDC based on Apache Directory Server that can be embedded in testcases
	/// or used from command line as a standalone KDC.
	/// <p>
	/// <b>From within testcases:</b>
	/// <p>
	/// MiniKdc sets 2 System properties when started and un-sets them when stopped:
	/// <ul>
	/// <li>java.security.krb5.conf: set to the MiniKDC real/host/port</li>
	/// <li>sun.security.krb5.debug: set to the debug value provided in the
	/// configuration</li>
	/// </ul>
	/// Because of this, multiple MiniKdc instances cannot be started in parallel.
	/// For example, running testcases in parallel that start a KDC each. To
	/// accomplish this a single MiniKdc should be used for all testcases running
	/// in parallel.
	/// <p>
	/// MiniKdc default configuration values are:
	/// <ul>
	/// <li>org.name=EXAMPLE (used to create the REALM)</li>
	/// <li>org.domain=COM (used to create the REALM)</li>
	/// <li>kdc.bind.address=localhost</li>
	/// <li>kdc.port=0 (ephemeral port)</li>
	/// <li>instance=DefaultKrbServer</li>
	/// <li>max.ticket.lifetime=86400000 (1 day)</li>
	/// <li>max.renewable.lifetime=604800000 (7 days)</li>
	/// <li>transport=TCP</li>
	/// <li>debug=false</li>
	/// </ul>
	/// The generated krb5.conf forces TCP connections.
	/// </remarks>
	public class MiniKdc
	{
		public const string JavaSecurityKrb5Conf = "java.security.krb5.conf";

		public const string SunSecurityKrb5Debug = "sun.security.krb5.debug";

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (args.Length < 4)
			{
				System.Console.Out.WriteLine("Arguments: <WORKDIR> <MINIKDCPROPERTIES> " + "<KEYTABFILE> [<PRINCIPALS>]+"
					);
				System.Environment.Exit(1);
			}
			FilePath workDir = new FilePath(args[0]);
			if (!workDir.Exists())
			{
				throw new RuntimeException("Specified work directory does not exists: " + workDir
					.GetAbsolutePath());
			}
			Properties conf = CreateConf();
			FilePath file = new FilePath(args[1]);
			if (!file.Exists())
			{
				throw new RuntimeException("Specified configuration does not exists: " + file.GetAbsolutePath
					());
			}
			Properties userConf = new Properties();
			InputStreamReader r = null;
			try
			{
				r = new InputStreamReader(new FileInputStream(file), Charsets.Utf8);
				userConf.Load(r);
			}
			finally
			{
				if (r != null)
				{
					r.Close();
				}
			}
			foreach (KeyValuePair<object, object> entry in userConf)
			{
				conf[entry.Key] = entry.Value;
			}
			Org.Apache.Hadoop.Minikdc.MiniKdc miniKdc = new Org.Apache.Hadoop.Minikdc.MiniKdc
				(conf, workDir);
			miniKdc.Start();
			FilePath krb5conf = new FilePath(workDir, "krb5.conf");
			if (miniKdc.GetKrb5conf().RenameTo(krb5conf))
			{
				FilePath keytabFile = new FilePath(args[2]).GetAbsoluteFile();
				string[] principals = new string[args.Length - 3];
				System.Array.Copy(args, 3, principals, 0, args.Length - 3);
				miniKdc.CreatePrincipal(keytabFile, principals);
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("Standalone MiniKdc Running");
				System.Console.Out.WriteLine("---------------------------------------------------"
					);
				System.Console.Out.WriteLine("  Realm           : " + miniKdc.GetRealm());
				System.Console.Out.WriteLine("  Running at      : " + miniKdc.GetHost() + ":" + miniKdc
					.GetHost());
				System.Console.Out.WriteLine("  krb5conf        : " + krb5conf);
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("  created keytab  : " + keytabFile);
				System.Console.Out.WriteLine("  with principals : " + Arrays.AsList(principals));
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine(" Do <CTRL-C> or kill <PID> to stop it");
				System.Console.Out.WriteLine("---------------------------------------------------"
					);
				System.Console.Out.WriteLine();
				Runtime.GetRuntime().AddShutdownHook(new _Thread_170(miniKdc));
			}
			else
			{
				throw new RuntimeException("Cannot rename KDC's krb5conf to " + krb5conf.GetAbsolutePath
					());
			}
		}

		private sealed class _Thread_170 : Thread
		{
			public _Thread_170(Org.Apache.Hadoop.Minikdc.MiniKdc miniKdc)
			{
				this.miniKdc = miniKdc;
			}

			public override void Run()
			{
				miniKdc.Stop();
			}

			private readonly Org.Apache.Hadoop.Minikdc.MiniKdc miniKdc;
		}

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Minikdc.MiniKdc
			));

		public const string OrgName = "org.name";

		public const string OrgDomain = "org.domain";

		public const string KdcBindAddress = "kdc.bind.address";

		public const string KdcPort = "kdc.port";

		public const string Instance = "instance";

		public const string MaxTicketLifetime = "max.ticket.lifetime";

		public const string MaxRenewableLifetime = "max.renewable.lifetime";

		public const string Transport = "transport";

		public const string Debug = "debug";

		private static readonly ICollection<string> Properties = new HashSet<string>();

		private static readonly Properties DefaultConfig = new Properties();

		static MiniKdc()
		{
			Properties.AddItem(OrgName);
			Properties.AddItem(OrgDomain);
			Properties.AddItem(KdcBindAddress);
			Properties.AddItem(KdcBindAddress);
			Properties.AddItem(KdcPort);
			Properties.AddItem(Instance);
			Properties.AddItem(Transport);
			Properties.AddItem(MaxTicketLifetime);
			Properties.AddItem(MaxRenewableLifetime);
			DefaultConfig.SetProperty(KdcBindAddress, "localhost");
			DefaultConfig.SetProperty(KdcPort, "0");
			DefaultConfig.SetProperty(Instance, "DefaultKrbServer");
			DefaultConfig.SetProperty(OrgName, "EXAMPLE");
			DefaultConfig.SetProperty(OrgDomain, "COM");
			DefaultConfig.SetProperty(Transport, "TCP");
			DefaultConfig.SetProperty(MaxTicketLifetime, "86400000");
			DefaultConfig.SetProperty(MaxRenewableLifetime, "604800000");
			DefaultConfig.SetProperty(Debug, "false");
		}

		/// <summary>Convenience method that returns MiniKdc default configuration.</summary>
		/// <remarks>
		/// Convenience method that returns MiniKdc default configuration.
		/// <p>
		/// The returned configuration is a copy, it can be customized before using
		/// it to create a MiniKdc.
		/// </remarks>
		/// <returns>a MiniKdc default configuration.</returns>
		public static Properties CreateConf()
		{
			return (Properties)DefaultConfig.Clone();
		}

		private Properties conf;

		private DirectoryService ds;

		private KdcServer kdc;

		private int port;

		private string realm;

		private FilePath workDir;

		private FilePath krb5conf;

		/// <summary>Creates a MiniKdc.</summary>
		/// <param name="conf">MiniKdc configuration.</param>
		/// <param name="workDir">
		/// working directory, it should be the build directory. Under
		/// this directory an ApacheDS working directory will be created, this
		/// directory will be deleted when the MiniKdc stops.
		/// </param>
		/// <exception cref="System.Exception">thrown if the MiniKdc could not be created.</exception>
		public MiniKdc(Properties conf, FilePath workDir)
		{
			if (!conf.Keys.ContainsAll(Properties))
			{
				ICollection<string> missingProperties = new HashSet<string>(Properties);
				missingProperties.RemoveAll(conf.Keys);
				throw new ArgumentException("Missing configuration properties: " + missingProperties
					);
			}
			this.workDir = new FilePath(workDir, System.Convert.ToString(Runtime.CurrentTimeMillis
				()));
			if (!workDir.Exists() && !workDir.Mkdirs())
			{
				throw new RuntimeException("Cannot create directory " + workDir);
			}
			Log.Info("Configuration:");
			Log.Info("---------------------------------------------------------------");
			foreach (KeyValuePair<object, object> entry in conf)
			{
				Log.Info("  {}: {}", entry.Key, entry.Value);
			}
			Log.Info("---------------------------------------------------------------");
			this.conf = conf;
			port = System.Convert.ToInt32(conf.GetProperty(KdcPort));
			if (port == 0)
			{
				Socket ss = Extensions.CreateServerSocket(0, 1, Extensions.GetAddressByName
					(conf.GetProperty(KdcBindAddress)));
				port = ss.GetLocalPort();
				ss.Close();
			}
			string orgName = conf.GetProperty(OrgName);
			string orgDomain = conf.GetProperty(OrgDomain);
			realm = orgName.ToUpper(Extensions.GetEnglishCulture()) + "." + orgDomain
				.ToUpper(Extensions.GetEnglishCulture());
		}

		/// <summary>Returns the port of the MiniKdc.</summary>
		/// <returns>the port of the MiniKdc.</returns>
		public virtual int GetPort()
		{
			return port;
		}

		/// <summary>Returns the host of the MiniKdc.</summary>
		/// <returns>the host of the MiniKdc.</returns>
		public virtual string GetHost()
		{
			return conf.GetProperty(KdcBindAddress);
		}

		/// <summary>Returns the realm of the MiniKdc.</summary>
		/// <returns>the realm of the MiniKdc.</returns>
		public virtual string GetRealm()
		{
			return realm;
		}

		public virtual FilePath GetKrb5conf()
		{
			return krb5conf;
		}

		/// <summary>Starts the MiniKdc.</summary>
		/// <exception cref="System.Exception">thrown if the MiniKdc could not be started.</exception>
		public virtual void Start()
		{
			lock (this)
			{
				if (kdc != null)
				{
					throw new RuntimeException("Already started");
				}
				InitDirectoryService();
				InitKDCServer();
			}
		}

		/// <exception cref="System.Exception"/>
		private void InitDirectoryService()
		{
			ds = new DefaultDirectoryService();
			ds.SetInstanceLayout(new InstanceLayout(workDir));
			CacheService cacheService = new CacheService();
			ds.SetCacheService(cacheService);
			// first load the schema
			InstanceLayout instanceLayout = ds.GetInstanceLayout();
			FilePath schemaPartitionDirectory = new FilePath(instanceLayout.GetPartitionsDirectory
				(), "schema");
			SchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(instanceLayout.GetPartitionsDirectory
				());
			extractor.ExtractOrCopy();
			SchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
			SchemaManager schemaManager = new DefaultSchemaManager(loader);
			schemaManager.LoadAllEnabled();
			ds.SetSchemaManager(schemaManager);
			// Init the LdifPartition with schema
			LdifPartition schemaLdifPartition = new LdifPartition(schemaManager);
			schemaLdifPartition.SetPartitionPath(schemaPartitionDirectory.ToURI());
			// The schema partition
			SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
			schemaPartition.SetWrappedPartition(schemaLdifPartition);
			ds.SetSchemaPartition(schemaPartition);
			JdbmPartition systemPartition = new JdbmPartition(ds.GetSchemaManager());
			systemPartition.SetId("system");
			systemPartition.SetPartitionPath(new FilePath(ds.GetInstanceLayout().GetPartitionsDirectory
				(), systemPartition.GetId()).ToURI());
			systemPartition.SetSuffixDn(new DN(ServerDNConstants.SystemDn));
			systemPartition.SetSchemaManager(ds.GetSchemaManager());
			ds.SetSystemPartition(systemPartition);
			ds.GetChangeLog().SetEnabled(false);
			ds.SetDenormalizeOpAttrsEnabled(true);
			ds.AddLast(new KeyDerivationInterceptor());
			// create one partition
			string orgName = conf.GetProperty(OrgName).ToLower(Extensions.GetEnglishCulture()
				);
			string orgDomain = conf.GetProperty(OrgDomain).ToLower(Extensions.GetEnglishCulture()
				);
			JdbmPartition partition = new JdbmPartition(ds.GetSchemaManager());
			partition.SetId(orgName);
			partition.SetPartitionPath(new FilePath(ds.GetInstanceLayout().GetPartitionsDirectory
				(), orgName).ToURI());
			partition.SetSuffixDn(new DN("dc=" + orgName + ",dc=" + orgDomain));
			ds.AddPartition(partition);
			// indexes
			ICollection<Index<object, object, string>> indexedAttributes = new HashSet<Index<
				object, object, string>>();
			indexedAttributes.AddItem(new JdbmIndex<string, Org.Apache.Directory.Api.Ldap.Model.Entry.Entry
				>("objectClass", false));
			indexedAttributes.AddItem(new JdbmIndex<string, Org.Apache.Directory.Api.Ldap.Model.Entry.Entry
				>("dc", false));
			indexedAttributes.AddItem(new JdbmIndex<string, Org.Apache.Directory.Api.Ldap.Model.Entry.Entry
				>("ou", false));
			partition.SetIndexedAttributes(indexedAttributes);
			// And start the ds
			ds.SetInstanceId(conf.GetProperty(Instance));
			ds.Startup();
			// context entry, after ds.startup()
			DN dn = new DN("dc=" + orgName + ",dc=" + orgDomain);
			Org.Apache.Directory.Api.Ldap.Model.Entry.Entry entry = ds.NewEntry(dn);
			entry.Add("objectClass", "top", "domain");
			entry.Add("dc", orgName);
			ds.GetAdminSession().Add(entry);
		}

		/// <exception cref="System.Exception"/>
		private void InitKDCServer()
		{
			string orgName = conf.GetProperty(OrgName);
			string orgDomain = conf.GetProperty(OrgDomain);
			string bindAddress = conf.GetProperty(KdcBindAddress);
			IDictionary<string, string> map = new Dictionary<string, string>();
			map["0"] = orgName.ToLower(Extensions.GetEnglishCulture());
			map["1"] = orgDomain.ToLower(Extensions.GetEnglishCulture());
			map["2"] = orgName.ToUpper(Extensions.GetEnglishCulture());
			map["3"] = orgDomain.ToUpper(Extensions.GetEnglishCulture());
			map["4"] = bindAddress;
			ClassLoader cl = Thread.CurrentThread().GetContextClassLoader();
			InputStream is1 = cl.GetResourceAsStream("minikdc.ldiff");
			SchemaManager schemaManager = ds.GetSchemaManager();
			LdifReader reader = null;
			try
			{
				string content = StrSubstitutor.Replace(IOUtils.ToString(is1), map);
				reader = new LdifReader(new StringReader(content));
				foreach (LdifEntry ldifEntry in reader)
				{
					ds.GetAdminSession().Add(new DefaultEntry(schemaManager, ldifEntry.GetEntry()));
				}
			}
			finally
			{
				IOUtils.CloseQuietly(reader);
				IOUtils.CloseQuietly(is1);
			}
			KerberosConfig kerberosConfig = new KerberosConfig();
			kerberosConfig.SetMaximumRenewableLifetime(long.Parse(conf.GetProperty(MaxRenewableLifetime
				)));
			kerberosConfig.SetMaximumTicketLifetime(long.Parse(conf.GetProperty(MaxTicketLifetime
				)));
			kerberosConfig.SetSearchBaseDn(string.Format("dc=%s,dc=%s", orgName, orgDomain));
			kerberosConfig.SetPaEncTimestampRequired(false);
			//kdc = new KdcServer(kerberosConfig);
			kdc = new KdcServer();
			kdc.SetDirectoryService(ds);
			// transport
			string transport = conf.GetProperty(Transport);
			if (transport.Trim().Equals("TCP"))
			{
				kdc.AddTransports(new TcpTransport(bindAddress, port, 3, 50));
			}
			else
			{
				if (transport.Trim().Equals("UDP"))
				{
					kdc.AddTransports(new UdpTransport(port));
				}
				else
				{
					throw new ArgumentException("Invalid transport: " + transport);
				}
			}
			kdc.SetServiceName(conf.GetProperty(Instance));
			kdc.Start();
			StringBuilder sb = new StringBuilder();
			InputStream is2 = cl.GetResourceAsStream("minikdc-krb5.conf");
			BufferedReader r = null;
			try
			{
				r = new BufferedReader(new InputStreamReader(is2, Charsets.Utf8));
				string line = r.ReadLine();
				while (line != null)
				{
					sb.Append(line).Append("{3}");
					line = r.ReadLine();
				}
			}
			finally
			{
				IOUtils.CloseQuietly(r);
				IOUtils.CloseQuietly(is2);
			}
			krb5conf = new FilePath(workDir, "krb5.conf").GetAbsoluteFile();
			FileUtils.WriteStringToFile(krb5conf, MessageFormat.Format(sb.ToString(), GetRealm
				(), GetHost(), Extensions.ToString(GetPort()), Runtime.GetProperty("line.separator"
				)));
			Runtime.SetProperty(JavaSecurityKrb5Conf, krb5conf.GetAbsolutePath());
			Runtime.SetProperty(SunSecurityKrb5Debug, conf.GetProperty(Debug, "false"));
			// refresh the config
			Type classRef;
			if (Runtime.GetProperty("java.vendor").Contains("IBM"))
			{
				classRef = Runtime.GetType("com.ibm.security.krb5.internal.Config");
			}
			else
			{
				classRef = Runtime.GetType("sun.security.krb5.Config");
			}
			MethodInfo refreshMethod = classRef.GetMethod("refresh", new Type[0]);
			refreshMethod.Invoke(classRef, new object[0]);
			Log.Info("MiniKdc listening at port: {}", GetPort());
			Log.Info("MiniKdc setting JVM krb5.conf to: {}", krb5conf.GetAbsolutePath());
		}

		/// <summary>Stops the MiniKdc</summary>
		public virtual void Stop()
		{
			lock (this)
			{
				if (kdc != null)
				{
					Runtime.GetProperties().Remove(JavaSecurityKrb5Conf);
					Runtime.GetProperties().Remove(SunSecurityKrb5Debug);
					kdc.Stop();
					try
					{
						ds.Shutdown();
					}
					catch (Exception ex)
					{
						Log.Error("Could not shutdown ApacheDS properly: {}", ex.ToString(), ex);
					}
				}
				Delete(workDir);
			}
		}

		private void Delete(FilePath f)
		{
			if (f.IsFile())
			{
				if (!f.Delete())
				{
					Log.Warn("WARNING: cannot delete file " + f.GetAbsolutePath());
				}
			}
			else
			{
				foreach (FilePath c in f.ListFiles())
				{
					Delete(c);
				}
				if (!f.Delete())
				{
					Log.Warn("WARNING: cannot delete directory " + f.GetAbsolutePath());
				}
			}
		}

		/// <summary>Creates a principal in the KDC with the specified user and password.</summary>
		/// <param name="principal">principal name, do not include the domain.</param>
		/// <param name="password">password.</param>
		/// <exception cref="System.Exception">thrown if the principal could not be created.</exception>
		public virtual void CreatePrincipal(string principal, string password)
		{
			lock (this)
			{
				string orgName = conf.GetProperty(OrgName);
				string orgDomain = conf.GetProperty(OrgDomain);
				string baseDn = "ou=users,dc=" + orgName.ToLower(Extensions.GetEnglishCulture()
					) + ",dc=" + orgDomain.ToLower(Extensions.GetEnglishCulture());
				string content = "dn: uid=" + principal + "," + baseDn + "\n" + "objectClass: top\n"
					 + "objectClass: person\n" + "objectClass: inetOrgPerson\n" + "objectClass: krb5principal\n"
					 + "objectClass: krb5kdcentry\n" + "cn: " + principal + "\n" + "sn: " + principal
					 + "\n" + "uid: " + principal + "\n" + "userPassword: " + password + "\n" + "krb5PrincipalName: "
					 + principal + "@" + GetRealm() + "\n" + "krb5KeyVersionNumber: 0";
				foreach (LdifEntry ldifEntry in new LdifReader(new StringReader(content)))
				{
					ds.GetAdminSession().Add(new DefaultEntry(ds.GetSchemaManager(), ldifEntry.GetEntry
						()));
				}
			}
		}

		/// <summary>Creates  multiple principals in the KDC and adds them to a keytab file.</summary>
		/// <param name="keytabFile">keytab file to add the created principal.s</param>
		/// <param name="principals">principals to add to the KDC, do not include the domain.
		/// 	</param>
		/// <exception cref="System.Exception">
		/// thrown if the principals or the keytab file could not be
		/// created.
		/// </exception>
		public virtual void CreatePrincipal(FilePath keytabFile, params string[] principals
			)
		{
			string generatedPassword = UUID.RandomUUID().ToString();
			Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab keytab = new Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab
				();
			IList<KeytabEntry> entries = new AList<KeytabEntry>();
			foreach (string principal in principals)
			{
				CreatePrincipal(principal, generatedPassword);
				principal = principal + "@" + GetRealm();
				KerberosTime timestamp = new KerberosTime();
				foreach (KeyValuePair<EncryptionType, EncryptionKey> entry in KerberosKeyFactory.
					GetKerberosKeys(principal, generatedPassword))
				{
					EncryptionKey ekey = entry.Value;
					byte keyVersion = unchecked((byte)ekey.GetKeyVersion());
					entries.AddItem(new KeytabEntry(principal, 1L, timestamp, keyVersion, ekey));
				}
			}
			keytab.SetEntries(entries);
			keytab.Write(keytabFile);
		}
	}
}
