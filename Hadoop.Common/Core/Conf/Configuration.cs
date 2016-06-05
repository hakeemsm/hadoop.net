using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security;
using System.Text;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Alias;
using Org.Apache.Hadoop.Util;

namespace Hadoop.Common.Core.Conf
{
	/// <summary>Provides access to configuration parameters.</summary>
	/// <remarks>
	/// Provides access to configuration parameters.
	/// <h4 id="Resources">Resources</h4>
	/// <p>Configurations are specified by resources. A resource contains a set of
	/// name/value pairs as XML data. Each resource is named by either a
	/// <code>String</code> or by a
	/// <see cref="Org.Apache.Hadoop.FS.Path"/>
	/// . If named by a <code>String</code>,
	/// then the classpath is examined for a file with that name.  If named by a
	/// <code>Path</code>, then the local filesystem is examined directly, without
	/// referring to the classpath.
	/// <p>Unless explicitly turned off, Hadoop by default specifies two
	/// resources, loaded in-order from the classpath: <ol>
	/// <li><tt>
	/// &lt;a href="
	/// <docRoot/>
	/// /../hadoop-project-dist/hadoop-common/core-default.xml"&gt;
	/// core-default.xml</a></tt>: Read-only defaults for hadoop.</li>
	/// <li><tt>core-site.xml</tt>: Site-specific configuration for a given hadoop
	/// installation.</li>
	/// </ol>
	/// Applications may add additional resources, which are loaded
	/// subsequent to these resources in the order they are added.
	/// <h4 id="FinalParams">Final Parameters</h4>
	/// <p>Configuration parameters may be declared <i>final</i>.
	/// Once a resource declares a value final, no subsequently-loaded
	/// resource can alter that value.
	/// For example, one might define a final parameter with:
	/// <tt><pre>
	/// &lt;property&gt;
	/// &lt;name&gt;dfs.hosts.include&lt;/name&gt;
	/// &lt;value&gt;/etc/hadoop/conf/hosts.include&lt;/value&gt;
	/// <b>&lt;final&gt;true&lt;/final&gt;</b>
	/// &lt;/property&gt;</pre></tt>
	/// Administrators typically define parameters as final in
	/// <tt>core-site.xml</tt> for values that user applications may not alter.
	/// <h4 id="VariableExpansion">Variable Expansion</h4>
	/// <p>Value strings are first processed for <i>variable expansion</i>. The
	/// available properties are:<ol>
	/// <li>Other properties defined in this Configuration; and, if a name is
	/// undefined here,</li>
	/// <li>Properties in
	/// <see cref="Runtime.GetProperties()"/>
	/// .</li>
	/// </ol>
	/// <p>For example, if a configuration resource contains the following property
	/// definitions:
	/// <tt><pre>
	/// &lt;property&gt;
	/// &lt;name&gt;basedir&lt;/name&gt;
	/// &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;tempdir&lt;/name&gt;
	/// &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
	/// &lt;/property&gt;</pre></tt>
	/// When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt>
	/// will be resolved to another property in this Configuration, while
	/// <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value
	/// of the System property with that name.
	/// By default, warnings will be given to any deprecated configuration
	/// parameters and these are suppressible by configuring
	/// <tt>log4j.logger.org.apache.hadoop.conf.Configuration.deprecation</tt> in
	/// log4j.properties file.
	/// </remarks>
	public class Configuration : IEnumerable<KeyValuePair<string, string>>, IWritable
	{
		private static readonly Org.Apache.Hadoop.Log Log = LogFactory.GetLog(typeof(Configuration
			));

		private static readonly Org.Apache.Hadoop.Log LogDeprecation = LogFactory.GetLog("org.apache.hadoop.conf.Configuration.deprecation"
			);

		private bool quietmode = true;

		private const string DefaultStringCheck = "testingforemptydefaultvalue";

		private bool allowNullValueProperties = false;

		private class Resource
		{
			private readonly object resource;

			private readonly string name;

			public Resource(object resource)
				: this(resource, resource.ToString())
			{
			}

			public Resource(object resource, string name)
			{
				this.resource = resource;
				this.name = name;
			}

			public virtual string GetName()
			{
				return name;
			}

			public virtual object GetResource()
			{
				return resource;
			}

			public override string ToString()
			{
				return name;
			}
		}

		/// <summary>List of configuration resources.</summary>
		private AList<Configuration.Resource> resources = new AList<Configuration.Resource
			>();

		/// <summary>
		/// The value reported as the setting resource when a key is set
		/// by code rather than a file resource by dumpConfiguration.
		/// </summary>
		internal const string UnknownResource = "Unknown";

		/// <summary>List of configuration parameters marked <b>final</b>.</summary>
		private ICollection<string> finalParameters = Collections.NewSetFromMap(new 
			ConcurrentHashMap<string, bool>());

		private bool loadDefaults = true;

		/// <summary>Configuration objects</summary>
		private static readonly WeakHashMap<Configuration, object> Registry = new WeakHashMap
			<Configuration, object>();

		/// <summary>List of default Resources.</summary>
		/// <remarks>
		/// List of default Resources. Resources are loaded in the order of the list
		/// entries
		/// </remarks>
		private static readonly CopyOnWriteArrayList<string> defaultResources = new CopyOnWriteArrayList
			<string>();

		private static readonly IDictionary<ClassLoader, IDictionary<string, WeakReference
			<Type>>> CacheClasses = new WeakHashMap<ClassLoader, IDictionary<string, WeakReference
			<Type>>>();

		/// <summary>
		/// Sentinel value to store negative cache results in
		/// <see cref="CacheClasses"/>
		/// .
		/// </summary>
		private static readonly Type NegativeCacheSentinel = typeof(Configuration.NegativeCacheSentinel
			);

		/// <summary>
		/// Stores the mapping of key to the resource which modifies or loads
		/// the key most recently
		/// </summary>
		private IDictionary<string, string[]> updatingResource;

		/// <summary>
		/// Class to keep the information about the keys which replace the deprecated
		/// ones.
		/// </summary>
		/// <remarks>
		/// Class to keep the information about the keys which replace the deprecated
		/// ones.
		/// This class stores the new keys which replace the deprecated keys and also
		/// gives a provision to have a custom message for each of the deprecated key
		/// that is being replaced. It also provides method to get the appropriate
		/// warning message which can be logged whenever the deprecated key is used.
		/// </remarks>
		private class DeprecatedKeyInfo
		{
			private readonly string[] newKeys;

			private readonly string customMessage;

			private readonly AtomicBoolean accessed = new AtomicBoolean(false);

			internal DeprecatedKeyInfo(string[] newKeys, string customMessage)
			{
				this.newKeys = newKeys;
				this.customMessage = customMessage;
			}

			/// <summary>Method to provide the warning message.</summary>
			/// <remarks>
			/// Method to provide the warning message. It gives the custom message if
			/// non-null, and default message otherwise.
			/// </remarks>
			/// <param name="key">the associated deprecated key.</param>
			/// <returns>message that is to be logged when a deprecated key is used.</returns>
			private string GetWarningMessage(string key)
			{
				string warningMessage;
				if (customMessage == null)
				{
					StringBuilder message = new StringBuilder(key);
					string deprecatedKeySuffix = " is deprecated. Instead, use ";
					message.Append(deprecatedKeySuffix);
					for (int i = 0; i < newKeys.Length; i++)
					{
						message.Append(newKeys[i]);
						if (i != newKeys.Length - 1)
						{
							message.Append(", ");
						}
					}
					warningMessage = message.ToString();
				}
				else
				{
					warningMessage = customMessage;
				}
				return warningMessage;
			}

			internal virtual bool GetAndSetAccessed()
			{
				return accessed.GetAndSet(true);
			}

			public virtual void ClearAccessed()
			{
				accessed.Set(false);
			}
		}

		/// <summary>A pending addition to the global set of deprecated keys.</summary>
		public class DeprecationDelta
		{
			private readonly string key;

			private readonly string[] newKeys;

			private readonly string customMessage;

			internal DeprecationDelta(string key, string[] newKeys, string customMessage)
			{
				Preconditions.CheckNotNull(key);
				Preconditions.CheckNotNull(newKeys);
				Preconditions.CheckArgument(newKeys.Length > 0);
				this.key = key;
				this.newKeys = newKeys;
				this.customMessage = customMessage;
			}

			public DeprecationDelta(string key, string newKey, string customMessage)
				: this(key, new string[] { newKey }, customMessage)
			{
			}

			public DeprecationDelta(string key, string newKey)
				: this(key, new string[] { newKey }, null)
			{
			}

			public virtual string GetKey()
			{
				return key;
			}

			public virtual string[] GetNewKeys()
			{
				return newKeys;
			}

			public virtual string GetCustomMessage()
			{
				return customMessage;
			}
		}

		/// <summary>The set of all keys which are deprecated.</summary>
		/// <remarks>
		/// The set of all keys which are deprecated.
		/// DeprecationContext objects are immutable.
		/// </remarks>
		private class DeprecationContext
		{
			/// <summary>
			/// Stores the deprecated keys, the new keys which replace the deprecated keys
			/// and custom message(if any provided).
			/// </summary>
			private readonly IDictionary<string, Configuration.DeprecatedKeyInfo> deprecatedKeyMap;

			/// <summary>Stores a mapping from superseding keys to the keys which they deprecate.
			/// 	</summary>
			private readonly IDictionary<string, string> reverseDeprecatedKeyMap;

			/// <summary>
			/// Create a new DeprecationContext by copying a previous DeprecationContext
			/// and adding some deltas.
			/// </summary>
			/// <param name="other">
			/// The previous deprecation context to copy, or null to start
			/// from nothing.
			/// </param>
			/// <param name="deltas">The deltas to apply.</param>
			internal DeprecationContext(Configuration.DeprecationContext other, Configuration.DeprecationDelta
				[] deltas)
			{
				Dictionary<string, Configuration.DeprecatedKeyInfo> newDeprecatedKeyMap = new Dictionary
					<string, Configuration.DeprecatedKeyInfo>();
				Dictionary<string, string> newReverseDeprecatedKeyMap = new Dictionary<string, string
					>();
				if (other != null)
				{
					foreach (KeyValuePair<string, Configuration.DeprecatedKeyInfo> entry in other.deprecatedKeyMap)
					{
						newDeprecatedKeyMap[entry.Key] = entry.Value;
					}
					foreach (KeyValuePair<string, string> entry_1 in other.reverseDeprecatedKeyMap)
					{
						newReverseDeprecatedKeyMap[entry_1.Key] = entry_1.Value;
					}
				}
				foreach (Configuration.DeprecationDelta delta in deltas)
				{
					if (!newDeprecatedKeyMap.Contains(delta.GetKey()))
					{
						Configuration.DeprecatedKeyInfo newKeyInfo = new Configuration.DeprecatedKeyInfo(
							delta.GetNewKeys(), delta.GetCustomMessage());
						newDeprecatedKeyMap[delta.key] = newKeyInfo;
						foreach (string newKey in delta.GetNewKeys())
						{
							newReverseDeprecatedKeyMap[newKey] = delta.key;
						}
					}
				}
				this.deprecatedKeyMap = UnmodifiableMap.Decorate(newDeprecatedKeyMap);
				this.reverseDeprecatedKeyMap = UnmodifiableMap.Decorate(newReverseDeprecatedKeyMap
					);
			}

			internal virtual IDictionary<string, Configuration.DeprecatedKeyInfo> GetDeprecatedKeyMap
				()
			{
				return deprecatedKeyMap;
			}

			internal virtual IDictionary<string, string> GetReverseDeprecatedKeyMap()
			{
				return reverseDeprecatedKeyMap;
			}
		}

		private static Configuration.DeprecationDelta[] defaultDeprecations = new Configuration.DeprecationDelta
			[] { new Configuration.DeprecationDelta("topology.script.file.name", CommonConfigurationKeys
			.NetTopologyScriptFileNameKey), new Configuration.DeprecationDelta("topology.script.number.args"
			, CommonConfigurationKeys.NetTopologyScriptNumberArgsKey), new Configuration.DeprecationDelta
			("hadoop.configured.node.mapping", CommonConfigurationKeys.NetTopologyConfiguredNodeMappingKey
			), new Configuration.DeprecationDelta("topology.node.switch.mapping.impl", CommonConfigurationKeys
			.NetTopologyNodeSwitchMappingImplKey), new Configuration.DeprecationDelta("dfs.df.interval"
			, CommonConfigurationKeys.FsDfIntervalKey), new Configuration.DeprecationDelta("hadoop.native.lib"
			, CommonConfigurationKeys.IoNativeLibAvailableKey), new Configuration.DeprecationDelta
			("fs.default.name", CommonConfigurationKeys.FsDefaultNameKey), new Configuration.DeprecationDelta
			("dfs.umaskmode", CommonConfigurationKeys.FsPermissionsUmaskKey), new Configuration.DeprecationDelta
			("dfs.nfs.exports.allowed.hosts", CommonConfigurationKeys.NfsExportsAllowedHostsKey
			) };

		/// <summary>The global DeprecationContext.</summary>
		private static AtomicReference<Configuration.DeprecationContext> deprecationContext
			 = new AtomicReference<Configuration.DeprecationContext>(new Configuration.DeprecationContext
			(null, defaultDeprecations));

		/// <summary>Adds a set of deprecated keys to the global deprecations.</summary>
		/// <remarks>
		/// Adds a set of deprecated keys to the global deprecations.
		/// This method is lockless.  It works by means of creating a new
		/// DeprecationContext based on the old one, and then atomically swapping in
		/// the new context.  If someone else updated the context in between us reading
		/// the old context and swapping in the new one, we try again until we win the
		/// race.
		/// </remarks>
		/// <param name="deltas">The deprecations to add.</param>
		public static void AddDeprecations(Configuration.DeprecationDelta[] deltas)
		{
			Configuration.DeprecationContext prev;
			Configuration.DeprecationContext next;
			do
			{
				prev = deprecationContext.Get();
				next = new Configuration.DeprecationContext(prev, deltas);
			}
			while (!deprecationContext.CompareAndSet(prev, next));
		}

		/// <summary>Adds the deprecated key to the global deprecation map.</summary>
		/// <remarks>
		/// Adds the deprecated key to the global deprecation map.
		/// It does not override any existing entries in the deprecation map.
		/// This is to be used only by the developers in order to add deprecation of
		/// keys, and attempts to call this method after loading resources once,
		/// would lead to <tt>UnsupportedOperationException</tt>
		/// If a key is deprecated in favor of multiple keys, they are all treated as
		/// aliases of each other, and setting any one of them resets all the others
		/// to the new value.
		/// If you have multiple deprecation entries to add, it is more efficient to
		/// use #addDeprecations(DeprecationDelta[] deltas) instead.
		/// </remarks>
		/// <param name="key"/>
		/// <param name="newKeys"/>
		/// <param name="customMessage"/>
		[Obsolete(@"use AddDeprecation(string, string, string) instead")]
		public static void AddDeprecation(string key, string[] newKeys, string customMessage
			)
		{
			AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
				(key, newKeys, customMessage) });
		}

		/// <summary>Adds the deprecated key to the global deprecation map.</summary>
		/// <remarks>
		/// Adds the deprecated key to the global deprecation map.
		/// It does not override any existing entries in the deprecation map.
		/// This is to be used only by the developers in order to add deprecation of
		/// keys, and attempts to call this method after loading resources once,
		/// would lead to <tt>UnsupportedOperationException</tt>
		/// If you have multiple deprecation entries to add, it is more efficient to
		/// use #addDeprecations(DeprecationDelta[] deltas) instead.
		/// </remarks>
		/// <param name="key"/>
		/// <param name="newKey"/>
		/// <param name="customMessage"/>
		public static void AddDeprecation(string key, string newKey, string customMessage
			)
		{
			AddDeprecation(key, new string[] { newKey }, customMessage);
		}

		/// <summary>
		/// Adds the deprecated key to the global deprecation map when no custom
		/// message is provided.
		/// </summary>
		/// <remarks>
		/// Adds the deprecated key to the global deprecation map when no custom
		/// message is provided.
		/// It does not override any existing entries in the deprecation map.
		/// This is to be used only by the developers in order to add deprecation of
		/// keys, and attempts to call this method after loading resources once,
		/// would lead to <tt>UnsupportedOperationException</tt>
		/// If a key is deprecated in favor of multiple keys, they are all treated as
		/// aliases of each other, and setting any one of them resets all the others
		/// to the new value.
		/// If you have multiple deprecation entries to add, it is more efficient to
		/// use #addDeprecations(DeprecationDelta[] deltas) instead.
		/// </remarks>
		/// <param name="key">Key that is to be deprecated</param>
		/// <param name="newKeys">list of keys that take up the values of deprecated key</param>
		[Obsolete(@"use AddDeprecation(string, string) instead")]
		public static void AddDeprecation(string key, string[] newKeys)
		{
			AddDeprecation(key, newKeys, null);
		}

		/// <summary>
		/// Adds the deprecated key to the global deprecation map when no custom
		/// message is provided.
		/// </summary>
		/// <remarks>
		/// Adds the deprecated key to the global deprecation map when no custom
		/// message is provided.
		/// It does not override any existing entries in the deprecation map.
		/// This is to be used only by the developers in order to add deprecation of
		/// keys, and attempts to call this method after loading resources once,
		/// would lead to <tt>UnsupportedOperationException</tt>
		/// If you have multiple deprecation entries to add, it is more efficient to
		/// use #addDeprecations(DeprecationDelta[] deltas) instead.
		/// </remarks>
		/// <param name="key">Key that is to be deprecated</param>
		/// <param name="newKey">key that takes up the value of deprecated key</param>
		public static void AddDeprecation(string key, string newKey)
		{
			AddDeprecation(key, new string[] { newKey }, null);
		}

		/// <summary>checks whether the given <code>key</code> is deprecated.</summary>
		/// <param name="key">the parameter which is to be checked for deprecation</param>
		/// <returns>
		/// <code>true</code> if the key is deprecated and
		/// <code>false</code> otherwise.
		/// </returns>
		public static bool IsDeprecated(string key)
		{
			return deprecationContext.Get().GetDeprecatedKeyMap().Contains(key);
		}

		/// <summary>
		/// Sets all deprecated properties that are not currently set but have a
		/// corresponding new property that is set.
		/// </summary>
		/// <remarks>
		/// Sets all deprecated properties that are not currently set but have a
		/// corresponding new property that is set. Useful for iterating the
		/// properties when all deprecated properties for currently set properties
		/// need to be present.
		/// </remarks>
		public virtual void SetDeprecatedProperties()
		{
			Configuration.DeprecationContext deprecations = deprecationContext.Get();
			Properties props = GetProps();
			Properties overlay = GetOverlay();
			foreach (KeyValuePair<string, Configuration.DeprecatedKeyInfo> entry in deprecations
				.GetDeprecatedKeyMap())
			{
				string depKey = entry.Key;
				if (!overlay.Contains(depKey))
				{
					foreach (string newKey in entry.Value.newKeys)
					{
						string val = overlay.GetProperty(newKey);
						if (val != null)
						{
							props.SetProperty(depKey, val);
							overlay.SetProperty(depKey, val);
							break;
						}
					}
				}
			}
		}

		/// <summary>
		/// Checks for the presence of the property <code>name</code> in the
		/// deprecation map.
		/// </summary>
		/// <remarks>
		/// Checks for the presence of the property <code>name</code> in the
		/// deprecation map. Returns the first of the list of new keys if present
		/// in the deprecation map or the <code>name</code> itself. If the property
		/// is not presently set but the property map contains an entry for the
		/// deprecated key, the value of the deprecated key is set as the value for
		/// the provided property name.
		/// </remarks>
		/// <param name="name">the property name</param>
		/// <returns>
		/// the first property in the list of properties mapping
		/// the <code>name</code> or the <code>name</code> itself.
		/// </returns>
		private string[] HandleDeprecation(Configuration.DeprecationContext deprecations, 
			string name)
		{
			if (null != name)
			{
				name = name.Trim();
			}
			AList<string> names = new AList<string>();
			if (IsDeprecated(name))
			{
				Configuration.DeprecatedKeyInfo keyInfo = deprecations.GetDeprecatedKeyMap()[name
					];
				WarnOnceIfDeprecated(deprecations, name);
				foreach (string newKey in keyInfo.newKeys)
				{
					if (newKey != null)
					{
						names.AddItem(newKey);
					}
				}
			}
			if (names.Count == 0)
			{
				names.AddItem(name);
			}
			foreach (string n in names)
			{
				string deprecatedKey = deprecations.GetReverseDeprecatedKeyMap()[n];
				if (deprecatedKey != null && !GetOverlay().Contains(n) && GetOverlay().Contains(deprecatedKey
					))
				{
					GetProps().SetProperty(n, GetOverlay().GetProperty(deprecatedKey));
					GetOverlay().SetProperty(n, GetOverlay().GetProperty(deprecatedKey));
				}
			}
			return Collections.ToArray(names, new string[names.Count]);
		}

		private void HandleDeprecation()
		{
			Log.Debug("Handling deprecation for all properties in config...");
			Configuration.DeprecationContext deprecations = deprecationContext.Get();
			ICollection<object> keys = new HashSet<object>();
			Collections.AddAll(keys, GetProps().Keys);
			foreach (object item in keys)
			{
				Log.Debug("Handling deprecation for " + (string)item);
				HandleDeprecation(deprecations, (string)item);
			}
		}

		static Configuration()
		{
			//print deprecation warning if hadoop-site.xml is found in classpath
			ClassLoader cL = Thread.CurrentThread().GetContextClassLoader();
			if (cL == null)
			{
				cL = typeof(Configuration).GetClassLoader();
			}
			if (cL.GetResource("hadoop-site.xml") != null)
			{
				Log.Warn("DEPRECATED: hadoop-site.xml found in the classpath. " + "Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
					 + "mapred-site.xml and hdfs-site.xml to override properties of " + "core-default.xml, mapred-default.xml and hdfs-default.xml "
					 + "respectively");
			}
			AddDefaultResource("core-default.xml");
			AddDefaultResource("core-site.xml");
		}

		private Properties properties;

		private Properties overlay;

		private ClassLoader classLoader;

		/// <summary>A new configuration.</summary>
		public Configuration()
			: this(true)
		{
		}

		/// <summary>
		/// A new configuration where the behavior of reading from the default
		/// resources can be turned off.
		/// </summary>
		/// <remarks>
		/// A new configuration where the behavior of reading from the default
		/// resources can be turned off.
		/// If the parameter
		/// <paramref name="loadDefaults"/>
		/// is false, the new instance
		/// will not load resources from the default files.
		/// </remarks>
		/// <param name="loadDefaults">specifies whether to load from the default files</param>
		public Configuration(bool loadDefaults)
		{
			{
				classLoader = Thread.CurrentThread().GetContextClassLoader();
				if (classLoader == null)
				{
					classLoader = typeof(Configuration).GetClassLoader();
				}
			}
			this.loadDefaults = loadDefaults;
			updatingResource = new ConcurrentHashMap<string, string[]>();
			lock (typeof(Configuration))
			{
				Registry[this] = null;
			}
		}

		/// <summary>A new configuration with the same settings cloned from another.</summary>
		/// <param name="other">the configuration from which to clone settings.</param>
		public Configuration(Configuration other)
		{
			{
				classLoader = Thread.CurrentThread().GetContextClassLoader();
				if (classLoader == null)
				{
					classLoader = typeof(Configuration).GetClassLoader();
				}
			}
			this.resources = (AList<Configuration.Resource>)other.resources.Clone();
			lock (other)
			{
				if (other.properties != null)
				{
					this.properties = (Properties)other.properties.Clone();
				}
				if (other.overlay != null)
				{
					this.overlay = (Properties)other.overlay.Clone();
				}
				this.updatingResource = new ConcurrentHashMap<string, string[]>(other.updatingResource
					);
				this.finalParameters = Collections.NewSetFromMap(new ConcurrentHashMap<string
					, bool>());
				Collections.AddAll(this.finalParameters, other.finalParameters);
			}
			lock (typeof(Configuration))
			{
				Registry[this] = null;
			}
			this.classLoader = other.classLoader;
			this.loadDefaults = other.loadDefaults;
			SetQuietMode(other.GetQuietMode());
		}

		/// <summary>Add a default resource.</summary>
		/// <remarks>
		/// Add a default resource. Resources are loaded in the order of the resources
		/// added.
		/// </remarks>
		/// <param name="name">file name. File should be present in the classpath.</param>
		public static void AddDefaultResource(string name)
		{
			lock (typeof(Configuration))
			{
				if (!defaultResources.Contains(name))
				{
					defaultResources.AddItem(name);
					foreach (Configuration conf in Registry.Keys)
					{
						if (conf.loadDefaults)
						{
							conf.ReloadConfiguration();
						}
					}
				}
			}
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="name">
		/// resource to be added, the classpath is examined for a file
		/// with that name.
		/// </param>
		public virtual void AddResource(string name)
		{
			AddResourceObject(new Configuration.Resource(name));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="url">
		/// url of the resource to be added, the local filesystem is
		/// examined directly to find the resource, without referring to
		/// the classpath.
		/// </param>
		public virtual void AddResource(Uri url)
		{
			AddResourceObject(new Configuration.Resource(url));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="file">
		/// file-path of resource to be added, the local filesystem is
		/// examined directly to find the resource, without referring to
		/// the classpath.
		/// </param>
		public virtual void AddResource(Path file)
		{
			AddResourceObject(new Configuration.Resource(file));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// WARNING: The contents of the InputStream will be cached, by this method.
		/// So use this sparingly because it does increase the memory consumption.
		/// </remarks>
		/// <param name="in">
		/// InputStream to deserialize the object from. In will be read from
		/// when a get or set is called next.  After it is read the stream will be
		/// closed.
		/// </param>
		public virtual void AddResource(InputStream @in)
		{
			AddResourceObject(new Configuration.Resource(@in));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="in">InputStream to deserialize the object from.</param>
		/// <param name="name">
		/// the name of the resource because InputStream.toString is not
		/// very descriptive some times.
		/// </param>
		public virtual void AddResource(InputStream @in, string name)
		{
			AddResourceObject(new Configuration.Resource(@in, name));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="conf">Configuration object from which to load properties</param>
		public virtual void AddResource(Configuration conf)
		{
			AddResourceObject(new Configuration.Resource(conf.GetProps()));
		}

		/// <summary>Reload configuration from previously added resources.</summary>
		/// <remarks>
		/// Reload configuration from previously added resources.
		/// This method will clear all the configuration read from the added
		/// resources, and final parameters. This will make the resources to
		/// be read again before accessing the values. Values that are added
		/// via set methods will overlay values read from the resources.
		/// </remarks>
		public virtual void ReloadConfiguration()
		{
			lock (this)
			{
				properties = null;
				// trigger reload
				finalParameters.Clear();
			}
		}

		// clear site-limits
		private void AddResourceObject(Configuration.Resource resource)
		{
			lock (this)
			{
				resources.AddItem(resource);
				// add to resources
				ReloadConfiguration();
			}
		}

		private const int MaxSubst = 20;

		private const int SubStartIdx = 0;

		private const int SubEndIdx = SubStartIdx + 1;

		/// <summary>
		/// This is a manual implementation of the following regex
		/// "\\$\\{[^\\}\\$\u0020]+\\}".
		/// </summary>
		/// <remarks>
		/// This is a manual implementation of the following regex
		/// "\\$\\{[^\\}\\$\u0020]+\\}". It can be 15x more efficient than
		/// a regex matcher as demonstrated by HADOOP-11506. This is noticeable with
		/// Hadoop apps building on the assumption Configuration#get is an O(1)
		/// hash table lookup, especially when the eval is a long string.
		/// </remarks>
		/// <param name="eval">a string that may contain variables requiring expansion.</param>
		/// <returns>
		/// a 2-element int array res such that
		/// eval.substring(res[0], res[1]) is "var" for the left-most occurrence of
		/// ${var} in eval. If no variable is found -1, -1 is returned.
		/// </returns>
		private static int[] FindSubVariable(string eval)
		{
			int[] result = new int[] { -1, -1 };
			int matchStart;
			int leftBrace;
			// scanning for a brace first because it's less frequent than $
			// that can occur in nested class names
			//
			for (matchStart = 1, leftBrace = eval.IndexOf('{', matchStart); leftBrace > 0 && 
				leftBrace + "{c".Length < eval.Length; leftBrace = eval.IndexOf('{', matchStart))
			{
				// minimum left brace position (follows '$')
				// right brace of a smallest valid expression "${c}"
				int matchedLen = 0;
				if (eval[leftBrace - 1] == '$')
				{
					int subStart = leftBrace + 1;
					// after '{'
					for (int i = subStart; i < eval.Length; i++)
					{
						switch (eval[i])
						{
							case '}':
							{
								if (matchedLen > 0)
								{
									// match
									result[SubStartIdx] = subStart;
									result[SubEndIdx] = subStart + matchedLen;
									goto match_loop_break;
								}
								goto case ' ';
							}

							case ' ':
							case '$':
							{
								// fall through to skip 1 char
								matchStart = i + 1;
								goto match_loop_continue;
							}

							default:
							{
								matchedLen++;
								break;
							}
						}
					}
					// scanned from "${"  to the end of eval, and no reset via ' ', '$':
					//    no match!
					goto match_loop_break;
				}
				else
				{
					// not a start of a variable
					//
					matchStart = leftBrace + 1;
				}
match_loop_continue: ;
			}
match_loop_break: ;
			return result;
		}

		/// <summary>
		/// Attempts to repeatedly expand the value
		/// <paramref name="expr"/>
		/// by replacing the
		/// left-most substring of the form "${var}" in the following precedence order
		/// <ol>
		/// <li>by the value of the Java system property "var" if defined</li>
		/// <li>by the value of the configuration key "var" if defined</li>
		/// </ol>
		/// If var is unbounded the current state of expansion "prefix${var}suffix" is
		/// returned.
		/// </summary>
		/// <param name="expr">the literal value of a config key</param>
		/// <returns>
		/// null if expr is null, otherwise the value resulting from expanding
		/// expr using the algorithm above.
		/// </returns>
		/// <exception cref="System.ArgumentException">
		/// when more than
		/// <see cref="MaxSubst"/>
		/// replacements are required
		/// </exception>
		private string SubstituteVars(string expr)
		{
			if (expr == null)
			{
				return null;
			}
			string eval = expr;
			for (int s = 0; s < MaxSubst; s++)
			{
				int[] varBounds = FindSubVariable(eval);
				if (varBounds[SubStartIdx] == -1)
				{
					return eval;
				}
				string var = Runtime.Substring(eval, varBounds[SubStartIdx], varBounds[SubEndIdx
					]);
				string val = null;
				try
				{
					val = Runtime.GetProperty(var);
				}
				catch (SecurityException se)
				{
					Log.Warn("Unexpected SecurityException in Configuration", se);
				}
				if (val == null)
				{
					val = GetRaw(var);
				}
				if (val == null)
				{
					return eval;
				}
				// return literal ${var}: var is unbound
				int dollar = varBounds[SubStartIdx] - "${".Length;
				int afterRightBrace = varBounds[SubEndIdx] + "}".Length;
				// substitute
				eval = Runtime.Substring(eval, 0, dollar) + val + Runtime.Substring
					(eval, afterRightBrace);
			}
			throw new InvalidOperationException("Variable substitution depth too large: " + MaxSubst
				 + " " + expr);
		}

		/// <summary>
		/// Get the value of the <code>name</code> property, <code>null</code> if
		/// no such property exists.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property, <code>null</code> if
		/// no such property exists. If the key is deprecated, it returns the value of
		/// the first key which replaces the deprecated key and is not null.
		/// Values are processed for <a href="#VariableExpansion">variable expansion</a>
		/// before being returned.
		/// </remarks>
		/// <param name="name">the property name, will be trimmed before get value.</param>
		/// <returns>
		/// the value of the <code>name</code> or its replacing property,
		/// or null if no such property exists.
		/// </returns>
		public virtual string Get(string name)
		{
			string[] names = HandleDeprecation(deprecationContext.Get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = SubstituteVars(GetProps().GetProperty(n));
			}
			return result;
		}

		/// <summary>Set Configuration to allow keys without values during setup.</summary>
		/// <remarks>
		/// Set Configuration to allow keys without values during setup.  Intended
		/// for use during testing.
		/// </remarks>
		/// <param name="val">If true, will allow Configuration to store keys without values</param>
		[VisibleForTesting]
		public virtual void SetAllowNullValueProperties(bool val)
		{
			this.allowNullValueProperties = val;
		}

		/// <summary>
		/// Return existence of the <code>name</code> property, but only for
		/// names which have no valid value, usually non-existent or commented
		/// out in XML.
		/// </summary>
		/// <param name="name">the property name</param>
		/// <returns>true if the property <code>name</code> exists without value</returns>
		[VisibleForTesting]
		public virtual bool OnlyKeyExists(string name)
		{
			string[] names = HandleDeprecation(deprecationContext.Get(), name);
			foreach (string n in names)
			{
				if (GetProps().GetProperty(n, DefaultStringCheck).Equals(DefaultStringCheck))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a trimmed <code>String</code>,
		/// <code>null</code> if no such property exists.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a trimmed <code>String</code>,
		/// <code>null</code> if no such property exists.
		/// If the key is deprecated, it returns the value of
		/// the first key which replaces the deprecated key and is not null
		/// Values are processed for <a href="#VariableExpansion">variable expansion</a>
		/// before being returned.
		/// </remarks>
		/// <param name="name">the property name.</param>
		/// <returns>
		/// the value of the <code>name</code> or its replacing property,
		/// or null if no such property exists.
		/// </returns>
		public virtual string GetTrimmed(string name)
		{
			string value = Get(name);
			if (null == value)
			{
				return null;
			}
			else
			{
				return value.Trim();
			}
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a trimmed <code>String</code>,
		/// <code>defaultValue</code> if no such property exists.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a trimmed <code>String</code>,
		/// <code>defaultValue</code> if no such property exists.
		/// See @{Configuration#getTrimmed} for more details.
		/// </remarks>
		/// <param name="name">the property name.</param>
		/// <param name="defaultValue">the property default value.</param>
		/// <returns>
		/// the value of the <code>name</code> or defaultValue
		/// if it is not set.
		/// </returns>
		public virtual string GetTrimmed(string name, string defaultValue)
		{
			string ret = GetTrimmed(name);
			return ret == null ? defaultValue : ret;
		}

		/// <summary>
		/// Get the value of the <code>name</code> property, without doing
		/// <a href="#VariableExpansion">variable expansion</a>.If the key is
		/// deprecated, it returns the value of the first key which replaces
		/// the deprecated key and is not null.
		/// </summary>
		/// <param name="name">the property name.</param>
		/// <returns>
		/// the value of the <code>name</code> property or
		/// its replacing property and null if no such property exists.
		/// </returns>
		public virtual string GetRaw(string name)
		{
			string[] names = HandleDeprecation(deprecationContext.Get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = GetProps().GetProperty(n);
			}
			return result;
		}

		/// <summary>
		/// Returns alternative names (non-deprecated keys or previously-set deprecated keys)
		/// for a given non-deprecated key.
		/// </summary>
		/// <remarks>
		/// Returns alternative names (non-deprecated keys or previously-set deprecated keys)
		/// for a given non-deprecated key.
		/// If the given key is deprecated, return null.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>alternative names.</returns>
		private string[] GetAlternativeNames(string name)
		{
			string[] altNames = null;
			Configuration.DeprecatedKeyInfo keyInfo = null;
			Configuration.DeprecationContext cur = deprecationContext.Get();
			string depKey = cur.GetReverseDeprecatedKeyMap()[name];
			if (depKey != null)
			{
				keyInfo = cur.GetDeprecatedKeyMap()[depKey];
				if (keyInfo.newKeys.Length > 0)
				{
					if (GetProps().Contains(depKey))
					{
						//if deprecated key is previously set explicitly
						IList<string> list = new AList<string>();
						Collections.AddAll(list, Arrays.AsList(keyInfo.newKeys));
						list.AddItem(depKey);
						altNames = Collections.ToArray(list, new string[list.Count]);
					}
					else
					{
						altNames = keyInfo.newKeys;
					}
				}
			}
			return altNames;
		}

		/// <summary>Set the <code>value</code> of the <code>name</code> property.</summary>
		/// <remarks>
		/// Set the <code>value</code> of the <code>name</code> property. If
		/// <code>name</code> is deprecated or there is a deprecated name associated to it,
		/// it sets the value to both names. Name will be trimmed before put into
		/// configuration.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		public virtual void Set(string name, string value)
		{
			Set(name, value, null);
		}

		/// <summary>Set the <code>value</code> of the <code>name</code> property.</summary>
		/// <remarks>
		/// Set the <code>value</code> of the <code>name</code> property. If
		/// <code>name</code> is deprecated, it also sets the <code>value</code> to
		/// the keys that replace the deprecated key. Name will be trimmed before put
		/// into configuration.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		/// <param name="source">
		/// the place that this configuration value came from
		/// (For debugging).
		/// </param>
		/// <exception cref="System.ArgumentException">when the value or name is null.</exception>
		public virtual void Set(string name, string value, string source)
		{
			Preconditions.CheckArgument(name != null, "Property name must not be null");
			Preconditions.CheckArgument(value != null, "The value of property " + name + " must not be null"
				);
			name = name.Trim();
			Configuration.DeprecationContext deprecations = deprecationContext.Get();
			if (deprecations.GetDeprecatedKeyMap().IsEmpty())
			{
				GetProps();
			}
			GetOverlay().SetProperty(name, value);
			GetProps().SetProperty(name, value);
			string newSource = (source == null ? "programatically" : source);
			if (!IsDeprecated(name))
			{
				updatingResource[name] = new string[] { newSource };
				string[] altNames = GetAlternativeNames(name);
				if (altNames != null)
				{
					foreach (string n in altNames)
					{
						if (!n.Equals(name))
						{
							GetOverlay().SetProperty(n, value);
							GetProps().SetProperty(n, value);
							updatingResource[n] = new string[] { newSource };
						}
					}
				}
			}
			else
			{
				string[] names = HandleDeprecation(deprecationContext.Get(), name);
				string altSource = "because " + name + " is deprecated";
				foreach (string n in names)
				{
					GetOverlay().SetProperty(n, value);
					GetProps().SetProperty(n, value);
					updatingResource[n] = new string[] { altSource };
				}
			}
		}

		private void WarnOnceIfDeprecated(Configuration.DeprecationContext deprecations, 
			string name)
		{
			Configuration.DeprecatedKeyInfo keyInfo = deprecations.GetDeprecatedKeyMap()[name
				];
			if (keyInfo != null && !keyInfo.GetAndSetAccessed())
			{
				LogDeprecation.Info(keyInfo.GetWarningMessage(name));
			}
		}

		/// <summary>Unset a previously set property.</summary>
		public virtual void Unset(string name)
		{
			lock (this)
			{
				string[] names = null;
				if (!IsDeprecated(name))
				{
					names = GetAlternativeNames(name);
					if (names == null)
					{
						names = new string[] { name };
					}
				}
				else
				{
					names = HandleDeprecation(deprecationContext.Get(), name);
				}
				foreach (string n in names)
				{
					GetOverlay().Remove(n);
					GetProps().Remove(n);
				}
			}
		}

		/// <summary>Sets a property if it is currently unset.</summary>
		/// <param name="name">the property name</param>
		/// <param name="value">the new value</param>
		public virtual void SetIfUnset(string name, string value)
		{
			lock (this)
			{
				if (Get(name) == null)
				{
					Set(name, value);
				}
			}
		}

		private Properties GetOverlay()
		{
			lock (this)
			{
				if (overlay == null)
				{
					overlay = new Properties();
				}
				return overlay;
			}
		}

		/// <summary>Get the value of the <code>name</code>.</summary>
		/// <remarks>
		/// Get the value of the <code>name</code>. If the key is deprecated,
		/// it returns the value of the first key which replaces the deprecated key
		/// and is not null.
		/// If no such property exists,
		/// then <code>defaultValue</code> is returned.
		/// </remarks>
		/// <param name="name">property name, will be trimmed before get value.</param>
		/// <param name="defaultValue">default value.</param>
		/// <returns>
		/// property value, or <code>defaultValue</code> if the property
		/// doesn't exist.
		/// </returns>
		public virtual string Get(string name, string defaultValue)
		{
			string[] names = HandleDeprecation(deprecationContext.Get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = SubstituteVars(GetProps().GetProperty(n, defaultValue));
			}
			return result;
		}

		/// <summary>Get the value of the <code>name</code> property as an <code>int</code>.</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as an <code>int</code>.
		/// If no such property exists, the provided default value is returned,
		/// or if the specified value is not a valid <code>int</code>,
		/// then an error is thrown.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <exception cref="System.FormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as an <code>int</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual int GetInt(string name, int defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			string hexString = GetHexDigits(valueString);
			if (hexString != null)
			{
				return System.Convert.ToInt32(hexString, 16);
			}
			return System.Convert.ToInt32(valueString);
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a set of comma-delimited
		/// <code>int</code> values.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a set of comma-delimited
		/// <code>int</code> values.
		/// If no such property exists, an empty array is returned.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <returns>
		/// property value interpreted as an array of comma-delimited
		/// <code>int</code> values
		/// </returns>
		public virtual int[] GetInts(string name)
		{
			string[] strings = GetTrimmedStrings(name);
			int[] ints = new int[strings.Length];
			for (int i = 0; i < strings.Length; i++)
			{
				ints[i] = System.Convert.ToInt32(strings[i]);
			}
			return ints;
		}

		/// <summary>Set the value of the <code>name</code> property to an <code>int</code>.</summary>
		/// <param name="name">property name.</param>
		/// <param name="value"><code>int</code> value of the property.</param>
		public virtual void SetInt(string name, int value)
		{
			Set(name, Extensions.ToString(value));
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>long</code>.</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>long</code>.
		/// If no such property exists, the provided default value is returned,
		/// or if the specified value is not a valid <code>long</code>,
		/// then an error is thrown.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <exception cref="System.FormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>long</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual long GetLong(string name, long defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			string hexString = GetHexDigits(valueString);
			if (hexString != null)
			{
				return long.Parse(hexString, 16);
			}
			return long.Parse(valueString);
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a <code>long</code> or
		/// human readable format.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>long</code> or
		/// human readable format. If no such property exists, the provided default
		/// value is returned, or if the specified value is not a valid
		/// <code>long</code> or human readable format, then an error is thrown. You
		/// can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
		/// t(tera), p(peta), e(exa)
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <exception cref="System.FormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>long</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual long GetLongBytes(string name, long defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return StringUtils.TraditionalBinaryPrefix.String2long(valueString);
		}

		private string GetHexDigits(string value)
		{
			bool negative = false;
			string str = value;
			string hexString = null;
			if (value.StartsWith("-"))
			{
				negative = true;
				str = Runtime.Substring(value, 1);
			}
			if (str.StartsWith("0x") || str.StartsWith("0X"))
			{
				hexString = Runtime.Substring(str, 2);
				if (negative)
				{
					hexString = "-" + hexString;
				}
				return hexString;
			}
			return null;
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>long</code>.</summary>
		/// <param name="name">property name.</param>
		/// <param name="value"><code>long</code> value of the property.</param>
		public virtual void SetLong(string name, long value)
		{
			Set(name, System.Convert.ToString(value));
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>float</code>.
		/// 	</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>float</code>.
		/// If no such property exists, the provided default value is returned,
		/// or if the specified value is not a valid <code>float</code>,
		/// then an error is thrown.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <exception cref="System.FormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>float</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual float GetFloat(string name, float defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return float.ParseFloat(valueString);
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>float</code>.
		/// 	</summary>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		public virtual void SetFloat(string name, float value)
		{
			Set(name, float.ToString(value));
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>double</code>.
		/// 	</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>double</code>.
		/// If no such property exists, the provided default value is returned,
		/// or if the specified value is not a valid <code>double</code>,
		/// then an error is thrown.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <exception cref="System.FormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>double</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual double GetDouble(string name, double defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return double.ParseDouble(valueString);
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>double</code>.
		/// 	</summary>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		public virtual void SetDouble(string name, double value)
		{
			Set(name, double.ToString(value));
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>boolean</code>.
		/// 	</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>boolean</code>.
		/// If no such property is specified, or if the specified value is not a valid
		/// <code>boolean</code>, then <code>defaultValue</code> is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <returns>
		/// property value as a <code>boolean</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual bool GetBoolean(string name, bool defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (null == valueString || valueString.IsEmpty())
			{
				return defaultValue;
			}
			if (StringUtils.EqualsIgnoreCase("true", valueString))
			{
				return true;
			}
			else
			{
				if (StringUtils.EqualsIgnoreCase("false", valueString))
				{
					return false;
				}
				else
				{
					return defaultValue;
				}
			}
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>boolean</code>.
		/// 	</summary>
		/// <param name="name">property name.</param>
		/// <param name="value"><code>boolean</code> value of the property.</param>
		public virtual void SetBoolean(string name, bool value)
		{
			Set(name, bool.ToString(value));
		}

		/// <summary>Set the given property, if it is currently unset.</summary>
		/// <param name="name">property name</param>
		/// <param name="value">new value</param>
		public virtual void SetBooleanIfUnset(string name, bool value)
		{
			SetIfUnset(name, bool.ToString(value));
		}

		/// <summary>Set the value of the <code>name</code> property to the given type.</summary>
		/// <remarks>
		/// Set the value of the <code>name</code> property to the given type. This
		/// is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <param name="value">new value</param>
		public virtual void SetEnum<T>(string name, T value)
			where T : Enum<T>
		{
			Set(name, value.ToString());
		}

		/// <summary>Return value matching this enumerated type.</summary>
		/// <remarks>
		/// Return value matching this enumerated type.
		/// Note that the returned value is trimmed by this method.
		/// </remarks>
		/// <param name="name">Property name</param>
		/// <param name="defaultValue">Value returned if no mapping exists</param>
		/// <exception cref="System.ArgumentException">
		/// If mapping is illegal for the type
		/// provided
		/// </exception>
		public virtual T GetEnum<T>(string name, T defaultValue)
			where T : Enum<T>
		{
			string val = GetTrimmed(name);
			return null == val ? defaultValue : Enum.ValueOf(defaultValue.GetDeclaringClass()
				, val);
		}

		[System.Serializable]
		internal sealed class ParsedTimeDuration
		{
			public static readonly Configuration.ParsedTimeDuration Ns = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration Us = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration Ms = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration S = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration M = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration H = new Configuration.ParsedTimeDuration
				();

			public static readonly Configuration.ParsedTimeDuration D = new Configuration.ParsedTimeDuration
				();

			internal abstract TimeUnit Unit();

			internal abstract string Suffix();

			internal static Configuration.ParsedTimeDuration UnitFor(string s)
			{
				foreach (Configuration.ParsedTimeDuration ptd in Values())
				{
					// iteration order is in decl order, so SECONDS matched last
					if (s.EndsWith(ptd.Suffix()))
					{
						return ptd;
					}
				}
				return null;
			}

			internal static Configuration.ParsedTimeDuration UnitFor(TimeUnit unit)
			{
				foreach (Configuration.ParsedTimeDuration ptd in Values())
				{
					if (ptd.Unit() == unit)
					{
						return ptd;
					}
				}
				return null;
			}
		}

		/// <summary>Set the value of <code>name</code> to the given time duration.</summary>
		/// <remarks>
		/// Set the value of <code>name</code> to the given time duration. This
		/// is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
		/// </remarks>
		/// <param name="name">Property name</param>
		/// <param name="value">Time duration</param>
		/// <param name="unit">Unit of time</param>
		public virtual void SetTimeDuration(string name, long value, TimeUnit unit)
		{
			Set(name, value + Configuration.ParsedTimeDuration.UnitFor(unit).Suffix());
		}

		/// <summary>Return time duration in the given time unit.</summary>
		/// <remarks>
		/// Return time duration in the given time unit. Valid units are encoded in
		/// properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
		/// (ms), seconds (s), minutes (m), hours (h), and days (d).
		/// </remarks>
		/// <param name="name">Property name</param>
		/// <param name="defaultValue">Value returned if no mapping exists.</param>
		/// <param name="unit">Unit to convert the stored property, if it exists.</param>
		/// <exception cref="System.FormatException">
		/// If the property stripped of its unit is not
		/// a number
		/// </exception>
		public virtual long GetTimeDuration(string name, long defaultValue, TimeUnit unit
			)
		{
			string vStr = Get(name);
			if (null == vStr)
			{
				return defaultValue;
			}
			vStr = vStr.Trim();
			Configuration.ParsedTimeDuration vUnit = Configuration.ParsedTimeDuration.UnitFor
				(vStr);
			if (null == vUnit)
			{
				Log.Warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
				vUnit = Configuration.ParsedTimeDuration.UnitFor(unit);
			}
			else
			{
				vStr = Runtime.Substring(vStr, 0, vStr.LastIndexOf(vUnit.Suffix()));
			}
			return unit.Convert(long.Parse(vStr), vUnit.Unit());
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>Pattern</code>.
		/// 	</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>Pattern</code>.
		/// If no such property is specified, or if the specified value is not a valid
		/// <code>Pattern</code>, then <code>DefaultValue</code> is returned.
		/// Note that the returned value is NOT trimmed by this method.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <param name="defaultValue">default value</param>
		/// <returns>property value as a compiled Pattern, or defaultValue</returns>
		public virtual Pattern GetPattern(string name, Pattern defaultValue
			)
		{
			string valString = Get(name);
			if (null == valString || valString.IsEmpty())
			{
				return defaultValue;
			}
			try
			{
				return Pattern.Compile(valString);
			}
			catch (PatternSyntaxException pse)
			{
				Log.Warn("Regular expression '" + valString + "' for property '" + name + "' not valid. Using default"
					, pse);
				return defaultValue;
			}
		}

		/// <summary>Set the given property to <code>Pattern</code>.</summary>
		/// <remarks>
		/// Set the given property to <code>Pattern</code>.
		/// If the pattern is passed as null, sets the empty pattern which results in
		/// further calls to getPattern(...) returning the default value.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <param name="pattern">new value</param>
		public virtual void SetPattern(string name, Pattern pattern)
		{
			System.Diagnostics.Debug.Assert(pattern != null, "Pattern cannot be null");
			Set(name, pattern.Pattern());
		}

		/// <summary>Gets information about why a property was set.</summary>
		/// <remarks>
		/// Gets information about why a property was set.  Typically this is the
		/// path to the resource objects (file, URL, etc.) the property came from, but
		/// it can also indicate that it was set programatically, or because of the
		/// command line.
		/// </remarks>
		/// <param name="name">- The property name to get the source of.</param>
		/// <returns>
		/// null - If the property or its source wasn't found. Otherwise,
		/// returns a list of the sources of the resource.  The older sources are
		/// the first ones in the list.  So for example if a configuration is set from
		/// the command line, and then written out to a file that is read back in the
		/// first entry would indicate that it was set from the command line, while
		/// the second one would indicate the file that the new configuration was read
		/// in from.
		/// </returns>
		[InterfaceStability.Unstable]
		public virtual string[] GetPropertySources(string name)
		{
			lock (this)
			{
				if (properties == null)
				{
					// If properties is null, it means a resource was newly added
					// but the props were cleared so as to load it upon future
					// requests. So lets force a load by asking a properties list.
					GetProps();
				}
				// Return a null right away if our properties still
				// haven't loaded or the resource mapping isn't defined
				if (properties == null || updatingResource == null)
				{
					return null;
				}
				else
				{
					string[] source = updatingResource[name];
					if (source == null)
					{
						return null;
					}
					else
					{
						return Arrays.CopyOf(source, source.Length);
					}
				}
			}
		}

		/// <summary>A class that represents a set of positive integer ranges.</summary>
		/// <remarks>
		/// A class that represents a set of positive integer ranges. It parses
		/// strings of the form: "2-3,5,7-" where ranges are separated by comma and
		/// the lower/upper bounds are separated by dash. Either the lower or upper
		/// bound may be omitted meaning all values up to or over. So the string
		/// above means 2, 3, 5, and 7, 8, 9, ...
		/// </remarks>
		public class IntegerRanges : IEnumerable<int>
		{
			private class Range
			{
				internal int start;

				internal int end;
			}

			private class RangeNumberIterator : IEnumerator<int>
			{
				internal IEnumerator<Configuration.IntegerRanges.Range> @internal;

				internal int at;

				internal int end;

				public RangeNumberIterator(IList<Configuration.IntegerRanges.Range> ranges)
				{
					if (ranges != null)
					{
						@internal = ranges.GetEnumerator();
					}
					at = -1;
					end = -2;
				}

				public override bool HasNext()
				{
					if (at <= end)
					{
						return true;
					}
					else
					{
						if (@internal != null)
						{
							return @internal.HasNext();
						}
					}
					return false;
				}

				public override int Next()
				{
					if (at <= end)
					{
						at++;
						return at - 1;
					}
					else
					{
						if (@internal != null)
						{
							Configuration.IntegerRanges.Range found = @internal.Next();
							if (found != null)
							{
								at = found.start;
								end = found.end;
								at++;
								return at - 1;
							}
						}
					}
					return null;
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}
			}

			internal IList<Configuration.IntegerRanges.Range> ranges = new AList<Configuration.IntegerRanges.Range
				>();

			public IntegerRanges()
			{
			}

			public IntegerRanges(string newValue)
			{
				StringTokenizer itr = new StringTokenizer(newValue, ",");
				while (itr.HasMoreTokens())
				{
					string rng = itr.NextToken().Trim();
					string[] parts = rng.Split("-", 3);
					if (parts.Length < 1 || parts.Length > 2)
					{
						throw new ArgumentException("integer range badly formed: " + rng);
					}
					Configuration.IntegerRanges.Range r = new Configuration.IntegerRanges.Range();
					r.start = ConvertToInt(parts[0], 0);
					if (parts.Length == 2)
					{
						r.end = ConvertToInt(parts[1], int.MaxValue);
					}
					else
					{
						r.end = r.start;
					}
					if (r.start > r.end)
					{
						throw new ArgumentException("IntegerRange from " + r.start + " to " + r.end + " is invalid"
							);
					}
					ranges.AddItem(r);
				}
			}

			/// <summary>Convert a string to an int treating empty strings as the default value.</summary>
			/// <param name="value">the string value</param>
			/// <param name="defaultValue">the value for if the string is empty</param>
			/// <returns>the desired integer</returns>
			private static int ConvertToInt(string value, int defaultValue)
			{
				string trim = value.Trim();
				if (trim.Length == 0)
				{
					return defaultValue;
				}
				return System.Convert.ToInt32(trim);
			}

			/// <summary>Is the given value in the set of ranges</summary>
			/// <param name="value">the value to check</param>
			/// <returns>is the value in the ranges?</returns>
			public virtual bool IsIncluded(int value)
			{
				foreach (Configuration.IntegerRanges.Range r in ranges)
				{
					if (r.start <= value && value <= r.end)
					{
						return true;
					}
				}
				return false;
			}

			/// <returns>true if there are no values in this range, else false.</returns>
			public virtual bool IsEmpty()
			{
				return ranges == null || ranges.IsEmpty();
			}

			public override string ToString()
			{
				StringBuilder result = new StringBuilder();
				bool first = true;
				foreach (Configuration.IntegerRanges.Range r in ranges)
				{
					if (first)
					{
						first = false;
					}
					else
					{
						result.Append(',');
					}
					result.Append(r.start);
					result.Append('-');
					result.Append(r.end);
				}
				return result.ToString();
			}

			public override IEnumerator<int> GetEnumerator()
			{
				return new Configuration.IntegerRanges.RangeNumberIterator(ranges);
			}
		}

		/// <summary>Parse the given attribute as a set of integer ranges</summary>
		/// <param name="name">the attribute name</param>
		/// <param name="defaultValue">the default value if it is not set</param>
		/// <returns>a new set of ranges from the configured value</returns>
		public virtual Configuration.IntegerRanges GetRange(string name, string defaultValue
			)
		{
			return new Configuration.IntegerRanges(Get(name, defaultValue));
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// a collection of <code>String</code>s.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// a collection of <code>String</code>s.
		/// If no such property is specified then empty collection is returned.
		/// <p>
		/// This is an optimized version of
		/// <see cref="GetStrings(string)"/>
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>property value as a collection of <code>String</code>s.</returns>
		public virtual ICollection<string> GetStringCollection(string name)
		{
			string valueString = Get(name);
			return StringUtils.GetStringCollection(valueString);
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s.
		/// If no such property is specified then <code>null</code> is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>
		/// property value as an array of <code>String</code>s,
		/// or <code>null</code>.
		/// </returns>
		public virtual string[] GetStrings(string name)
		{
			string valueString = Get(name);
			return StringUtils.GetStrings(valueString);
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s.
		/// If no such property is specified then default value is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">The default value</param>
		/// <returns>
		/// property value as an array of <code>String</code>s,
		/// or default value.
		/// </returns>
		public virtual string[] GetStrings(string name, params string[] defaultValue)
		{
			string valueString = Get(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			else
			{
				return StringUtils.GetStrings(valueString);
			}
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// If no such property is specified then empty <code>Collection</code> is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>property value as a collection of <code>String</code>s, or empty <code>Collection</code>
		/// 	</returns>
		public virtual ICollection<string> GetTrimmedStringCollection(string name)
		{
			string valueString = Get(name);
			if (null == valueString)
			{
				ICollection<string> empty = new AList<string>();
				return empty;
			}
			return StringUtils.GetTrimmedStringCollection(valueString);
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// If no such property is specified then an empty array is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>
		/// property value as an array of trimmed <code>String</code>s,
		/// or empty array.
		/// </returns>
		public virtual string[] GetTrimmedStrings(string name)
		{
			string valueString = Get(name);
			return StringUtils.GetTrimmedStrings(valueString);
		}

		/// <summary>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// </summary>
		/// <remarks>
		/// Get the comma delimited values of the <code>name</code> property as
		/// an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
		/// If no such property is specified then default value is returned.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultValue">The default value</param>
		/// <returns>
		/// property value as an array of trimmed <code>String</code>s,
		/// or default value.
		/// </returns>
		public virtual string[] GetTrimmedStrings(string name, params string[] defaultValue
			)
		{
			string valueString = Get(name);
			if (null == valueString)
			{
				return defaultValue;
			}
			else
			{
				return StringUtils.GetTrimmedStrings(valueString);
			}
		}

		/// <summary>
		/// Set the array of string values for the <code>name</code> property as
		/// as comma delimited values.
		/// </summary>
		/// <param name="name">property name.</param>
		/// <param name="values">The values</param>
		public virtual void SetStrings(string name, params string[] values)
		{
			Set(name, StringUtils.ArrayToString(values));
		}

		/// <summary>Get the value for a known password configuration element.</summary>
		/// <remarks>
		/// Get the value for a known password configuration element.
		/// In order to enable the elimination of clear text passwords in config,
		/// this method attempts to resolve the property name as an alias through
		/// the CredentialProvider API and conditionally fallsback to config.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <returns>password</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual char[] GetPassword(string name)
		{
			char[] pass = null;
			pass = GetPasswordFromCredentialProviders(name);
			if (pass == null)
			{
				pass = GetPasswordFromConfig(name);
			}
			return pass;
		}

		/// <summary>
		/// Try and resolve the provided element name as a credential provider
		/// alias.
		/// </summary>
		/// <param name="name">alias of the provisioned credential</param>
		/// <returns>password or null if not found</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual char[] GetPasswordFromCredentialProviders(string name)
		{
			char[] pass = null;
			try
			{
				IList<CredentialProvider> providers = CredentialProviderFactory.GetProviders(this
					);
				if (providers != null)
				{
					foreach (CredentialProvider provider in providers)
					{
						try
						{
							CredentialProvider.CredentialEntry entry = provider.GetCredentialEntry(name);
							if (entry != null)
							{
								pass = entry.GetCredential();
								break;
							}
						}
						catch (IOException ioe)
						{
							throw new IOException("Can't get key " + name + " from key provider" + "of type: "
								 + provider.GetType().FullName + ".", ioe);
						}
					}
				}
			}
			catch (IOException ioe)
			{
				throw new IOException("Configuration problem with provider path.", ioe);
			}
			return pass;
		}

		/// <summary>Fallback to clear text passwords in configuration.</summary>
		/// <param name="name"/>
		/// <returns>clear text password or null</returns>
		protected internal virtual char[] GetPasswordFromConfig(string name)
		{
			char[] pass = null;
			if (GetBoolean(CredentialProvider.ClearTextFallback, true))
			{
				string passStr = Get(name);
				if (passStr != null)
				{
					pass = passStr.ToCharArray();
				}
			}
			return pass;
		}

		/// <summary>
		/// Get the socket address for <code>hostProperty</code> as a
		/// <code>InetSocketAddress</code>.
		/// </summary>
		/// <remarks>
		/// Get the socket address for <code>hostProperty</code> as a
		/// <code>InetSocketAddress</code>. If <code>hostProperty</code> is
		/// <code>null</code>, <code>addressProperty</code> will be used. This
		/// is useful for cases where we want to differentiate between host
		/// bind address and address clients should use to establish connection.
		/// </remarks>
		/// <param name="hostProperty">bind host property name.</param>
		/// <param name="addressProperty">address property name.</param>
		/// <param name="defaultAddressValue">the default value</param>
		/// <param name="defaultPort">the default port</param>
		/// <returns>InetSocketAddress</returns>
		public virtual IPEndPoint GetSocketAddr(string hostProperty, string addressProperty
			, string defaultAddressValue, int defaultPort)
		{
			IPEndPoint bindAddr = GetSocketAddr(addressProperty, defaultAddressValue, defaultPort
				);
			string host = Get(hostProperty);
			if (host == null || host.IsEmpty())
			{
				return bindAddr;
			}
			return NetUtils.CreateSocketAddr(host, bindAddr.Port, hostProperty);
		}

		/// <summary>
		/// Get the socket address for <code>name</code> property as a
		/// <code>InetSocketAddress</code>.
		/// </summary>
		/// <param name="name">property name.</param>
		/// <param name="defaultAddress">the default value</param>
		/// <param name="defaultPort">the default port</param>
		/// <returns>InetSocketAddress</returns>
		public virtual IPEndPoint GetSocketAddr(string name, string defaultAddress, int defaultPort
			)
		{
			string address = GetTrimmed(name, defaultAddress);
			return NetUtils.CreateSocketAddr(address, defaultPort, name);
		}

		/// <summary>
		/// Set the socket address for the <code>name</code> property as
		/// a <code>host:port</code>.
		/// </summary>
		public virtual void SetSocketAddr(string name, IPEndPoint addr)
		{
			Set(name, NetUtils.GetHostPortString(addr));
		}

		/// <summary>
		/// Set the socket address a client can use to connect for the
		/// <code>name</code> property as a <code>host:port</code>.
		/// </summary>
		/// <remarks>
		/// Set the socket address a client can use to connect for the
		/// <code>name</code> property as a <code>host:port</code>.  The wildcard
		/// address is replaced with the local host's address. If the host and address
		/// properties are configured the host component of the address will be combined
		/// with the port component of the addr to generate the address.  This is to allow
		/// optional control over which host name is used in multi-home bind-host
		/// cases where a host can have multiple names
		/// </remarks>
		/// <param name="hostProperty">the bind-host configuration name</param>
		/// <param name="addressProperty">the service address configuration name</param>
		/// <param name="defaultAddressValue">the service default address configuration value
		/// 	</param>
		/// <param name="addr">InetSocketAddress of the service listener</param>
		/// <returns>InetSocketAddress for clients to connect</returns>
		public virtual IPEndPoint UpdateConnectAddr(string hostProperty, string addressProperty
			, string defaultAddressValue, IPEndPoint addr)
		{
			string host = Get(hostProperty);
			string connectHostPort = GetTrimmed(addressProperty, defaultAddressValue);
			if (host == null || host.IsEmpty() || connectHostPort == null || connectHostPort.
				IsEmpty())
			{
				//not our case, fall back to original logic
				return UpdateConnectAddr(addressProperty, addr);
			}
			string connectHost = connectHostPort.Split(":")[0];
			// Create connect address using client address hostname and server port.
			return UpdateConnectAddr(addressProperty, NetUtils.CreateSocketAddrForHost(connectHost
				, addr.Port));
		}

		/// <summary>
		/// Set the socket address a client can use to connect for the
		/// <code>name</code> property as a <code>host:port</code>.
		/// </summary>
		/// <remarks>
		/// Set the socket address a client can use to connect for the
		/// <code>name</code> property as a <code>host:port</code>.  The wildcard
		/// address is replaced with the local host's address.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="addr">InetSocketAddress of a listener to store in the given property
		/// 	</param>
		/// <returns>InetSocketAddress for clients to connect</returns>
		public virtual IPEndPoint UpdateConnectAddr(string name, IPEndPoint addr)
		{
			IPEndPoint connectAddr = NetUtils.GetConnectAddress(addr);
			SetSocketAddr(name, connectAddr);
			return connectAddr;
		}

		/// <summary>Load a class by name.</summary>
		/// <param name="name">the class name.</param>
		/// <returns>the class object.</returns>
		/// <exception cref="System.TypeLoadException">if the class is not found.</exception>
		public virtual Type GetClassByName(string name)
		{
			Type ret = GetClassByNameOrNull(name);
			if (ret == null)
			{
				throw new TypeLoadException("Class " + name + " not found");
			}
			return ret;
		}

		/// <summary>
		/// Load a class by name, returning null rather than throwing an exception
		/// if it couldn't be loaded.
		/// </summary>
		/// <remarks>
		/// Load a class by name, returning null rather than throwing an exception
		/// if it couldn't be loaded. This is to avoid the overhead of creating
		/// an exception.
		/// </remarks>
		/// <param name="name">the class name</param>
		/// <returns>the class object, or null if it could not be found.</returns>
		public virtual Type GetClassByNameOrNull(string name)
		{
			IDictionary<string, WeakReference<Type>> map;
			lock (CacheClasses)
			{
				map = CacheClasses[classLoader];
				if (map == null)
				{
					map = Collections.SynchronizedMap(new WeakHashMap<string, WeakReference<Type
						>>());
					CacheClasses[classLoader] = map;
				}
			}
			Type clazz = null;
			WeakReference<Type> @ref = map[name];
			if (@ref != null)
			{
				clazz = @ref.Get();
			}
			if (clazz == null)
			{
				try
				{
					clazz = Runtime.GetType(name, true, classLoader);
				}
				catch (TypeLoadException)
				{
					// Leave a marker that the class isn't found
					map[name] = new WeakReference<Type>(NegativeCacheSentinel);
					return null;
				}
				// two putters can race here, but they'll put the same class
				map[name] = new WeakReference<Type>(clazz);
				return clazz;
			}
			else
			{
				if (clazz == NegativeCacheSentinel)
				{
					return null;
				}
				else
				{
					// not found
					// cache hit
					return clazz;
				}
			}
		}

		/// <summary>
		/// Get the value of the <code>name</code> property
		/// as an array of <code>Class</code>.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property
		/// as an array of <code>Class</code>.
		/// The value of the property specifies a list of comma separated class names.
		/// If no such property is specified, then <code>defaultValue</code> is
		/// returned.
		/// </remarks>
		/// <param name="name">the property name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <returns>
		/// property value as a <code>Class[]</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual Type[] GetClasses(string name, params Type[] defaultValue)
		{
			string[] classnames = GetTrimmedStrings(name);
			if (classnames == null)
			{
				return defaultValue;
			}
			try
			{
				Type[] classes = new Type[classnames.Length];
				for (int i = 0; i < classnames.Length; i++)
				{
					classes[i] = GetClassByName(classnames[i]);
				}
				return classes;
			}
			catch (TypeLoadException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>Get the value of the <code>name</code> property as a <code>Class</code>.
		/// 	</summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>Class</code>.
		/// If no such property is specified, then <code>defaultValue</code> is
		/// returned.
		/// </remarks>
		/// <param name="name">the class name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <returns>
		/// property value as a <code>Class</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual Type GetClass(string name, Type defaultValue)
		{
			string valueString = GetTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			try
			{
				return GetClassByName(valueString);
			}
			catch (TypeLoadException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a <code>Class</code>
		/// implementing the interface specified by <code>xface</code>.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>Class</code>
		/// implementing the interface specified by <code>xface</code>.
		/// If no such property is specified, then <code>defaultValue</code> is
		/// returned.
		/// An exception is thrown if the returned class does not implement the named
		/// interface.
		/// </remarks>
		/// <param name="name">the class name.</param>
		/// <param name="defaultValue">default value.</param>
		/// <param name="xface">the interface implemented by the named class.</param>
		/// <returns>
		/// property value as a <code>Class</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual Type GetClass<U>(string name, Type defaultValue)
		{
			System.Type xface = typeof(U);
			try
			{
				Type theClass = GetClass(name, defaultValue);
				if (theClass != null && !xface.IsAssignableFrom(theClass))
				{
					throw new RuntimeException(theClass + " not " + xface.FullName);
				}
				else
				{
					if (theClass != null)
					{
						return theClass.AsSubclass(xface);
					}
					else
					{
						return null;
					}
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>
		/// Get the value of the <code>name</code> property as a <code>List</code>
		/// of objects implementing the interface specified by <code>xface</code>.
		/// </summary>
		/// <remarks>
		/// Get the value of the <code>name</code> property as a <code>List</code>
		/// of objects implementing the interface specified by <code>xface</code>.
		/// An exception is thrown if any of the classes does not exist, or if it does
		/// not implement the named interface.
		/// </remarks>
		/// <param name="name">the property name.</param>
		/// <param name="xface">
		/// the interface implemented by the classes named by
		/// <code>name</code>.
		/// </param>
		/// <returns>a <code>List</code> of objects implementing <code>xface</code>.</returns>
		public virtual IList<U> GetInstances<U>(string name)
		{
			System.Type xface = typeof(U);
			IList<U> ret = new AList<U>();
			Type[] classes = GetClasses(name);
			foreach (Type cl in classes)
			{
				if (!xface.IsAssignableFrom(cl))
				{
					throw new RuntimeException(cl + " does not implement " + xface);
				}
				ret.AddItem((U)ReflectionUtils.NewInstance(cl, this));
			}
			return ret;
		}

		/// <summary>
		/// Set the value of the <code>name</code> property to the name of a
		/// <code>theClass</code> implementing the given interface <code>xface</code>.
		/// </summary>
		/// <remarks>
		/// Set the value of the <code>name</code> property to the name of a
		/// <code>theClass</code> implementing the given interface <code>xface</code>.
		/// An exception is thrown if <code>theClass</code> does not implement the
		/// interface <code>xface</code>.
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="theClass">property value.</param>
		/// <param name="xface">the interface implemented by the named class.</param>
		public virtual void SetClass(string name, Type theClass, Type xface)
		{
			if (!xface.IsAssignableFrom(theClass))
			{
				throw new RuntimeException(theClass + " not " + xface.FullName);
			}
			Set(name, theClass.FullName);
		}

		/// <summary>
		/// Get a local file under a directory named by <i>dirsProp</i> with
		/// the given <i>path</i>.
		/// </summary>
		/// <remarks>
		/// Get a local file under a directory named by <i>dirsProp</i> with
		/// the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
		/// then one is chosen based on <i>path</i>'s hash code.  If the selected
		/// directory does not exist, an attempt is made to create it.
		/// </remarks>
		/// <param name="dirsProp">directory in which to locate the file.</param>
		/// <param name="path">file-path.</param>
		/// <returns>local file under the directory with the given path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPath(string dirsProp, string path)
		{
			string[] dirs = GetTrimmedStrings(dirsProp);
			int hashCode = path.GetHashCode();
			FileSystem fs = FileSystem.GetLocal(this);
			for (int i = 0; i < dirs.Length; i++)
			{
				// try each local dir
				int index = (hashCode + i & int.MaxValue) % dirs.Length;
				Path file = new Path(dirs[index], path);
				Path dir = file.GetParent();
				if (fs.Mkdirs(dir) || fs.Exists(dir))
				{
					return file;
				}
			}
			Log.Warn("Could not make " + path + " in local directories from " + dirsProp);
			for (int i_1 = 0; i_1 < dirs.Length; i_1++)
			{
				int index = (hashCode + i_1 & int.MaxValue) % dirs.Length;
				Log.Warn(dirsProp + "[" + index + "]=" + dirs[index]);
			}
			throw new IOException("No valid local directories in property: " + dirsProp);
		}

		/// <summary>
		/// Get a local file name under a directory named in <i>dirsProp</i> with
		/// the given <i>path</i>.
		/// </summary>
		/// <remarks>
		/// Get a local file name under a directory named in <i>dirsProp</i> with
		/// the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
		/// then one is chosen based on <i>path</i>'s hash code.  If the selected
		/// directory does not exist, an attempt is made to create it.
		/// </remarks>
		/// <param name="dirsProp">directory in which to locate the file.</param>
		/// <param name="path">file-path.</param>
		/// <returns>local file under the directory with the given path.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FilePath GetFile(string dirsProp, string path)
		{
			string[] dirs = GetTrimmedStrings(dirsProp);
			int hashCode = path.GetHashCode();
			for (int i = 0; i < dirs.Length; i++)
			{
				// try each local dir
				int index = (hashCode + i & int.MaxValue) % dirs.Length;
				FilePath file = new FilePath(dirs[index], path);
				FilePath dir = file.GetParentFile();
				if (dir.Exists() || dir.Mkdirs())
				{
					return file;
				}
			}
			throw new IOException("No valid local directories in property: " + dirsProp);
		}

		/// <summary>
		/// Get the
		/// <see cref="System.Uri"/>
		/// for the named resource.
		/// </summary>
		/// <param name="name">resource name.</param>
		/// <returns>the url for the named resource.</returns>
		public virtual Uri GetResource(string name)
		{
			return classLoader.GetResource(name);
		}

		/// <summary>
		/// Get an input stream attached to the configuration resource with the
		/// given <code>name</code>.
		/// </summary>
		/// <param name="name">configuration resource name.</param>
		/// <returns>an input stream attached to the resource.</returns>
		public virtual InputStream GetConfResourceAsInputStream(string name)
		{
			try
			{
				Uri url = GetResource(name);
				if (url == null)
				{
					Log.Info(name + " not found");
					return null;
				}
				else
				{
					Log.Info("found resource " + name + " at " + url);
				}
				return url.OpenStream();
			}
			catch (Exception)
			{
				return null;
			}
		}

		/// <summary>
		/// Get a
		/// <see cref="System.IO.StreamReader"/>
		/// attached to the configuration resource with the
		/// given <code>name</code>.
		/// </summary>
		/// <param name="name">configuration resource name.</param>
		/// <returns>a reader attached to the resource.</returns>
		public virtual StreamReader GetConfResourceAsReader(string name)
		{
			try
			{
				Uri url = GetResource(name);
				if (url == null)
				{
					Log.Info(name + " not found");
					return null;
				}
				else
				{
					Log.Info("found resource " + name + " at " + url);
				}
				return new InputStreamReader(url.OpenStream(), Charsets.Utf8);
			}
			catch (Exception)
			{
				return null;
			}
		}

		/// <summary>Get the set of parameters marked final.</summary>
		/// <returns>final parameter set.</returns>
		public virtual ICollection<string> GetFinalParameters()
		{
			ICollection<string> setFinalParams = Collections.NewSetFromMap(new ConcurrentHashMap
				<string, bool>());
			Collections.AddAll(setFinalParams, finalParameters);
			return setFinalParams;
		}

		protected internal virtual Properties GetProps()
		{
			lock (this)
			{
				if (properties == null)
				{
					properties = new Properties();
					IDictionary<string, string[]> backup = new ConcurrentHashMap<string, string[]>(updatingResource
						);
					LoadResources(properties, resources, quietmode);
					if (overlay != null)
					{
						properties.PutAll(overlay);
						foreach (KeyValuePair<object, object> item in overlay)
						{
							string key = (string)item.Key;
							string[] source = backup[key];
							if (source != null)
							{
								updatingResource[key] = source;
							}
						}
					}
				}
				return properties;
			}
		}

		/// <summary>Return the number of keys in the configuration.</summary>
		/// <returns>number of keys in the configuration.</returns>
		public virtual int Size()
		{
			return GetProps().Count;
		}

		/// <summary>Clears all keys from the configuration.</summary>
		public virtual void Clear()
		{
			GetProps().Clear();
			GetOverlay().Clear();
		}

		/// <summary>
		/// Get an
		/// <see cref="System.Collections.IEnumerator{E}"/>
		/// to go through the list of <code>String</code>
		/// key-value pairs in the configuration.
		/// </summary>
		/// <returns>an iterator over the entries.</returns>
		public virtual IEnumerator<KeyValuePair<string, string>> GetEnumerator()
		{
			// Get a copy of just the string to string pairs. After the old object
			// methods that allow non-strings to be put into configurations are removed,
			// we could replace properties with a Map<String,String> and get rid of this
			// code.
			IDictionary<string, string> result = new Dictionary<string, string>();
			foreach (KeyValuePair<object, object> item in GetProps())
			{
				if (item.Key is string && item.Value is string)
				{
					result[(string)item.Key] = (string)item.Value;
				}
			}
			return result.GetEnumerator();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private Document Parse(DocumentBuilder builder, Uri url)
		{
			if (!quietmode)
			{
				Log.Debug("parsing URL " + url);
			}
			if (url == null)
			{
				return null;
			}
			return Parse(builder, url.OpenStream(), url.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private Document Parse(DocumentBuilder builder, InputStream @is, string systemId)
		{
			if (!quietmode)
			{
				Log.Debug("parsing input stream " + @is);
			}
			if (@is == null)
			{
				return null;
			}
			try
			{
				return (systemId == null) ? builder.Parse(@is) : builder.Parse(@is, systemId);
			}
			finally
			{
				@is.Close();
			}
		}

		private void LoadResources(Properties properties, AList<Configuration.Resource> resources
			, bool quiet)
		{
			if (loadDefaults)
			{
				foreach (string resource in defaultResources)
				{
					LoadResource(properties, new Configuration.Resource(resource), quiet);
				}
				//support the hadoop-site.xml as a deprecated case
				if (GetResource("hadoop-site.xml") != null)
				{
					LoadResource(properties, new Configuration.Resource("hadoop-site.xml"), quiet);
				}
			}
			for (int i = 0; i < resources.Count; i++)
			{
				Configuration.Resource ret = LoadResource(properties, resources[i], quiet);
				if (ret != null)
				{
					resources.Set(i, ret);
				}
			}
		}

		private Configuration.Resource LoadResource(Properties properties, Configuration.Resource
			 wrapper, bool quiet)
		{
			string name = UnknownResource;
			try
			{
				object resource = wrapper.GetResource();
				name = wrapper.GetName();
				DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
				//ignore all comments inside the xml file
				docBuilderFactory.SetIgnoringComments(true);
				//allow includes in the xml file
				docBuilderFactory.SetNamespaceAware(true);
				try
				{
					docBuilderFactory.SetXIncludeAware(true);
				}
				catch (NotSupportedException e)
				{
					Log.Error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory 
						+ ":" + e, e);
				}
				DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
				Document doc = null;
				Element root = null;
				bool returnCachedProperties = false;
				if (resource is Uri)
				{
					// an URL resource
					doc = Parse(builder, (Uri)resource);
				}
				else
				{
					if (resource is string)
					{
						// a CLASSPATH resource
						Uri url = GetResource((string)resource);
						doc = Parse(builder, url);
					}
					else
					{
						if (resource is Path)
						{
							// a file resource
							// Can't use FileSystem API or we get an infinite loop
							// since FileSystem uses Configuration API.  Use java.io.File instead.
							FilePath file = new FilePath(((Path)resource).ToUri().GetPath()).GetAbsoluteFile(
								);
							if (file.Exists())
							{
								if (!quiet)
								{
									Log.Debug("parsing File " + file);
								}
								doc = Parse(builder, new BufferedInputStream(new FileInputStream(file)), ((Path)resource
									).ToString());
							}
						}
						else
						{
							if (resource is InputStream)
							{
								doc = Parse(builder, (InputStream)resource, null);
								returnCachedProperties = true;
							}
							else
							{
								if (resource is Properties)
								{
									Overlay(properties, (Properties)resource);
								}
								else
								{
									if (resource is Element)
									{
										root = (Element)resource;
									}
								}
							}
						}
					}
				}
				if (root == null)
				{
					if (doc == null)
					{
						if (quiet)
						{
							return null;
						}
						throw new RuntimeException(resource + " not found");
					}
					root = doc.GetDocumentElement();
				}
				Properties toAddTo = properties;
				if (returnCachedProperties)
				{
					toAddTo = new Properties();
				}
				if (!"configuration".Equals(root.GetTagName()))
				{
					Log.Fatal("bad conf file: top-level element not <configuration>");
				}
				NodeList props = root.GetChildNodes();
				Configuration.DeprecationContext deprecations = deprecationContext.Get();
				for (int i = 0; i < props.GetLength(); i++)
				{
					Node propNode = props.Item(i);
					if (!(propNode is Element))
					{
						continue;
					}
					Element prop = (Element)propNode;
					if ("configuration".Equals(prop.GetTagName()))
					{
						LoadResource(toAddTo, new Configuration.Resource(prop, name), quiet);
						continue;
					}
					if (!"property".Equals(prop.GetTagName()))
					{
						Log.Warn("bad conf file: element not <property>");
					}
					NodeList fields = prop.GetChildNodes();
					string attr = null;
					string value = null;
					bool finalParameter = false;
					List<string> source = new List<string>();
					for (int j = 0; j < fields.GetLength(); j++)
					{
						Node fieldNode = fields.Item(j);
						if (!(fieldNode is Element))
						{
							continue;
						}
						Element field = (Element)fieldNode;
						if ("name".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							attr = StringInterner.WeakIntern(((Org.W3c.Dom.Text)field.GetFirstChild()).GetData
								().Trim());
						}
						if ("value".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							value = StringInterner.WeakIntern(((Org.W3c.Dom.Text)field.GetFirstChild()).GetData
								());
						}
						if ("final".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							finalParameter = "true".Equals(((Org.W3c.Dom.Text)field.GetFirstChild()).GetData(
								));
						}
						if ("source".Equals(field.GetTagName()) && field.HasChildNodes())
						{
							source.AddItem(StringInterner.WeakIntern(((Org.W3c.Dom.Text)field.GetFirstChild()
								).GetData()));
						}
					}
					source.AddItem(name);
					// Ignore this parameter if it has already been marked as 'final'
					if (attr != null)
					{
						if (deprecations.GetDeprecatedKeyMap().Contains(attr))
						{
							Configuration.DeprecatedKeyInfo keyInfo = deprecations.GetDeprecatedKeyMap()[attr
								];
							keyInfo.ClearAccessed();
							foreach (string key in keyInfo.newKeys)
							{
								// update new keys with deprecated key's value 
								LoadProperty(toAddTo, name, key, value, finalParameter, Collections.ToArray
									(source, new string[source.Count]));
							}
						}
						else
						{
							LoadProperty(toAddTo, name, attr, value, finalParameter, Collections.ToArray
								(source, new string[source.Count]));
						}
					}
				}
				if (returnCachedProperties)
				{
					Overlay(properties, toAddTo);
					return new Configuration.Resource(toAddTo, name);
				}
				return null;
			}
			catch (IOException e)
			{
				Log.Fatal("error parsing conf " + name, e);
				throw new RuntimeException(e);
			}
			catch (DOMException e)
			{
				Log.Fatal("error parsing conf " + name, e);
				throw new RuntimeException(e);
			}
			catch (SAXException e)
			{
				Log.Fatal("error parsing conf " + name, e);
				throw new RuntimeException(e);
			}
			catch (ParserConfigurationException e)
			{
				Log.Fatal("error parsing conf " + name, e);
				throw new RuntimeException(e);
			}
		}

		private void Overlay(Properties to, Properties from)
		{
			foreach (KeyValuePair<object, object> entry in from)
			{
				to[entry.Key] = entry.Value;
			}
		}

		private void LoadProperty(Properties properties, string name, string attr, string
			 value, bool finalParameter, string[] source)
		{
			if (value != null || allowNullValueProperties)
			{
				if (!finalParameters.Contains(attr))
				{
					if (value == null && allowNullValueProperties)
					{
						value = DefaultStringCheck;
					}
					properties.SetProperty(attr, value);
					if (source != null)
					{
						updatingResource[attr] = source;
					}
				}
				else
				{
					if (!value.Equals(properties.GetProperty(attr)))
					{
						Log.Warn(name + ":an attempt to override final parameter: " + attr + ";  Ignoring."
							);
					}
				}
			}
			if (finalParameter && attr != null)
			{
				finalParameters.AddItem(attr);
			}
		}

		/// <summary>
		/// Write out the non-default properties in this configuration to the given
		/// <see cref="System.IO.OutputStream"/>
		/// using UTF-8 encoding.
		/// </summary>
		/// <param name="out">the output stream to write to.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteXml(OutputStream @out)
		{
			WriteXml(new OutputStreamWriter(@out, "UTF-8"));
		}

		/// <summary>
		/// Write out the non-default properties in this configuration to the given
		/// <see cref="System.IO.TextWriter"/>
		/// .
		/// </summary>
		/// <param name="out">the writer to write to.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteXml(TextWriter @out)
		{
			Document doc = AsXmlDocument();
			try
			{
				DOMSource source = new DOMSource(doc);
				StreamResult result = new StreamResult(@out);
				TransformerFactory transFactory = TransformerFactory.NewInstance();
				Transformer transformer = transFactory.NewTransformer();
				// Important to not hold Configuration log while writing result, since
				// 'out' may be an HDFS stream which needs to lock this configuration
				// from another thread.
				transformer.Transform(source, result);
			}
			catch (TransformerException te)
			{
				throw new IOException(te);
			}
		}

		/// <summary>Return the XML DOM corresponding to this Configuration.</summary>
		/// <exception cref="System.IO.IOException"/>
		private Document AsXmlDocument()
		{
			lock (this)
			{
				Document doc;
				try
				{
					doc = DocumentBuilderFactory.NewInstance().NewDocumentBuilder().NewDocument();
				}
				catch (ParserConfigurationException pe)
				{
					throw new IOException(pe);
				}
				Element conf = doc.CreateElement("configuration");
				doc.AppendChild(conf);
				conf.AppendChild(doc.CreateTextNode("\n"));
				HandleDeprecation();
				//ensure properties is set and deprecation is handled
				for (Enumeration<object> e = properties.Keys; e.MoveNext(); )
				{
					string name = (string)e.Current;
					object @object = properties[name];
					string value = null;
					if (@object is string)
					{
						value = (string)@object;
					}
					else
					{
						continue;
					}
					Element propNode = doc.CreateElement("property");
					conf.AppendChild(propNode);
					Element nameNode = doc.CreateElement("name");
					nameNode.AppendChild(doc.CreateTextNode(name));
					propNode.AppendChild(nameNode);
					Element valueNode = doc.CreateElement("value");
					valueNode.AppendChild(doc.CreateTextNode(value));
					propNode.AppendChild(valueNode);
					if (updatingResource != null)
					{
						string[] sources = updatingResource[name];
						if (sources != null)
						{
							foreach (string s in sources)
							{
								Element sourceNode = doc.CreateElement("source");
								sourceNode.AppendChild(doc.CreateTextNode(s));
								propNode.AppendChild(sourceNode);
							}
						}
					}
					conf.AppendChild(doc.CreateTextNode("\n"));
				}
				return doc;
			}
		}

		/// <summary>
		/// Writes out all the parameters and their properties (final and resource) to
		/// the given
		/// <see cref="System.IO.TextWriter"/>
		/// The format of the output would be
		/// { "properties" : [ {key1,value1,key1.isFinal,key1.resource}, {key2,value2,
		/// key2.isFinal,key2.resource}... ] }
		/// It does not output the parameters of the configuration object which is
		/// loaded from an input stream.
		/// </summary>
		/// <param name="out">the Writer to write to</param>
		/// <exception cref="System.IO.IOException"/>
		public static void DumpConfiguration(Configuration config, TextWriter @out)
		{
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.CreateJsonGenerator(@out);
			dumpGenerator.WriteStartObject();
			dumpGenerator.WriteFieldName("properties");
			dumpGenerator.WriteStartArray();
			dumpGenerator.Flush();
			lock (config)
			{
				foreach (KeyValuePair<object, object> item in config.GetProps())
				{
					dumpGenerator.WriteStartObject();
					dumpGenerator.WriteStringField("key", (string)item.Key);
					dumpGenerator.WriteStringField("value", config.Get((string)item.Key));
					dumpGenerator.WriteBooleanField("isFinal", config.finalParameters.Contains(item.Key
						));
					string[] resources = config.updatingResource[item.Key];
					string resource = UnknownResource;
					if (resources != null && resources.Length > 0)
					{
						resource = resources[0];
					}
					dumpGenerator.WriteStringField("resource", resource);
					dumpGenerator.WriteEndObject();
				}
			}
			dumpGenerator.WriteEndArray();
			dumpGenerator.WriteEndObject();
			dumpGenerator.Flush();
		}

		/// <summary>
		/// Get the
		/// <see cref="ClassLoader"/>
		/// for this job.
		/// </summary>
		/// <returns>the correct class loader.</returns>
		public virtual ClassLoader GetClassLoader()
		{
			return classLoader;
		}

		/// <summary>Set the class loader that will be used to load the various objects.</summary>
		/// <param name="classLoader">the new class loader.</param>
		public virtual void SetClassLoader(ClassLoader classLoader)
		{
			this.classLoader = classLoader;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Configuration: ");
			if (loadDefaults)
			{
				ToString(defaultResources, sb);
				if (resources.Count > 0)
				{
					sb.Append(", ");
				}
			}
			ToString(resources, sb);
			return sb.ToString();
		}

		private void ToString<T>(IList<T> resources, StringBuilder sb)
		{
			ListIterator<T> i = resources.ListIterator();
			while (i.HasNext())
			{
				if (i.NextIndex() != 0)
				{
					sb.Append(", ");
				}
				sb.Append(i.Next());
			}
		}

		/// <summary>Set the quietness-mode.</summary>
		/// <remarks>
		/// Set the quietness-mode.
		/// In the quiet-mode, error and informational messages might not be logged.
		/// </remarks>
		/// <param name="quietmode">
		/// <code>true</code> to set quiet-mode on, <code>false</code>
		/// to turn it off.
		/// </param>
		public virtual void SetQuietMode(bool quietmode)
		{
			lock (this)
			{
				this.quietmode = quietmode;
			}
		}

		internal virtual bool GetQuietMode()
		{
			lock (this)
			{
				return this.quietmode;
			}
		}

		/// <summary>For debugging.</summary>
		/// <remarks>For debugging.  List non-default properties to the terminal and exit.</remarks>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new Configuration().WriteXml(System.Console.Out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			Clear();
			int size = WritableUtils.ReadVInt(@in);
			for (int i = 0; i < size; ++i)
			{
				string key = Text.ReadString(@in);
				string value = Text.ReadString(@in);
				Set(key, value);
				string[] sources = WritableUtils.ReadCompressedStringArray(@in);
				if (sources != null)
				{
					updatingResource[key] = sources;
				}
			}
		}

		//@Override
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			Properties props = GetProps();
			WritableUtils.WriteVInt(@out, props.Count);
			foreach (KeyValuePair<object, object> item in props)
			{
				Text.WriteString(@out, (string)item.Key);
				Text.WriteString(@out, (string)item.Value);
				WritableUtils.WriteCompressedStringArray(@out, updatingResource[item.Key]);
			}
		}

		/// <summary>get keys matching the the regex</summary>
		/// <param name="regex"/>
		/// <returns>Map<String,String> with matching keys</returns>
		public virtual IDictionary<string, string> GetValByRegex(string regex)
		{
			Pattern p = Pattern.Compile(regex);
			IDictionary<string, string> result = new Dictionary<string, string>();
			Matcher m;
			foreach (KeyValuePair<object, object> item in GetProps())
			{
				if (item.Key is string && item.Value is string)
				{
					m = p.Matcher((string)item.Key);
					if (m.Find())
					{
						// match
						result[(string)item.Key] = SubstituteVars(GetProps().GetProperty((string)item.Key
							));
					}
				}
			}
			return result;
		}

		/// <summary>
		/// A unique class which is used as a sentinel value in the caching
		/// for getClassByName.
		/// </summary>
		/// <remarks>
		/// A unique class which is used as a sentinel value in the caching
		/// for getClassByName.
		/// <seealso>Configuration#getClassByNameOrNull(String)</seealso>
		/// </remarks>
		private abstract class NegativeCacheSentinel
		{
		}

		public static void DumpDeprecatedKeys()
		{
			Configuration.DeprecationContext deprecations = deprecationContext.Get();
			foreach (KeyValuePair<string, Configuration.DeprecatedKeyInfo> entry in deprecations
				.GetDeprecatedKeyMap())
			{
				StringBuilder newKeys = new StringBuilder();
				foreach (string newKey in entry.Value.newKeys)
				{
					newKeys.Append(newKey).Append("\t");
				}
				System.Console.Out.WriteLine(entry.Key + "\t" + newKeys.ToString());
			}
		}

		/// <summary>Returns whether or not a deprecated name has been warned.</summary>
		/// <remarks>
		/// Returns whether or not a deprecated name has been warned. If the name is not
		/// deprecated then always return false
		/// </remarks>
		public static bool HasWarnedDeprecation(string name)
		{
			Configuration.DeprecationContext deprecations = deprecationContext.Get();
			if (deprecations.GetDeprecatedKeyMap().Contains(name))
			{
				if (deprecations.GetDeprecatedKeyMap()[name].accessed.Get())
				{
					return true;
				}
			}
			return false;
		}
	}
}
