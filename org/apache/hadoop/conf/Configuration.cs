using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>Provides access to configuration parameters.</summary>
	/// <remarks>
	/// Provides access to configuration parameters.
	/// <h4 id="Resources">Resources</h4>
	/// <p>Configurations are specified by resources. A resource contains a set of
	/// name/value pairs as XML data. Each resource is named by either a
	/// <code>String</code> or by a
	/// <see cref="org.apache.hadoop.fs.Path"/>
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
	/// <see cref="Sharpen.Runtime.getProperties()"/>
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
	public class Configuration : System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair
		<string, string>>, org.apache.hadoop.io.Writable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration
			)));

		private static readonly org.apache.commons.logging.Log LOG_DEPRECATION = org.apache.commons.logging.LogFactory
			.getLog("org.apache.hadoop.conf.Configuration.deprecation");

		private bool quietmode = true;

		private const string DEFAULT_STRING_CHECK = "testingforemptydefaultvalue";

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

			public virtual string getName()
			{
				return name;
			}

			public virtual object getResource()
			{
				return resource;
			}

			public override string ToString()
			{
				return name;
			}
		}

		/// <summary>List of configuration resources.</summary>
		private System.Collections.Generic.List<org.apache.hadoop.conf.Configuration.Resource
			> resources = new System.Collections.Generic.List<org.apache.hadoop.conf.Configuration.Resource
			>();

		/// <summary>
		/// The value reported as the setting resource when a key is set
		/// by code rather than a file resource by dumpConfiguration.
		/// </summary>
		internal const string UNKNOWN_RESOURCE = "Unknown";

		/// <summary>List of configuration parameters marked <b>final</b>.</summary>
		private System.Collections.Generic.ICollection<string> finalParameters = java.util.Collections
			.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<string, bool>());

		private bool loadDefaults = true;

		/// <summary>Configuration objects</summary>
		private static readonly java.util.WeakHashMap<org.apache.hadoop.conf.Configuration
			, object> REGISTRY = new java.util.WeakHashMap<org.apache.hadoop.conf.Configuration
			, object>();

		/// <summary>List of default Resources.</summary>
		/// <remarks>
		/// List of default Resources. Resources are loaded in the order of the list
		/// entries
		/// </remarks>
		private static readonly java.util.concurrent.CopyOnWriteArrayList<string> defaultResources
			 = new java.util.concurrent.CopyOnWriteArrayList<string>();

		private static readonly System.Collections.Generic.IDictionary<java.lang.ClassLoader
			, System.Collections.Generic.IDictionary<string, java.lang.@ref.WeakReference<java.lang.Class
			>>> CACHE_CLASSES = new java.util.WeakHashMap<java.lang.ClassLoader, System.Collections.Generic.IDictionary
			<string, java.lang.@ref.WeakReference<java.lang.Class>>>();

		/// <summary>
		/// Sentinel value to store negative cache results in
		/// <see cref="CACHE_CLASSES"/>
		/// .
		/// </summary>
		private static readonly java.lang.Class NEGATIVE_CACHE_SENTINEL = Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.conf.Configuration.NegativeCacheSentinel));

		/// <summary>
		/// Stores the mapping of key to the resource which modifies or loads
		/// the key most recently
		/// </summary>
		private System.Collections.Generic.IDictionary<string, string[]> updatingResource;

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

			private readonly java.util.concurrent.atomic.AtomicBoolean accessed = new java.util.concurrent.atomic.AtomicBoolean
				(false);

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
			private string getWarningMessage(string key)
			{
				string warningMessage;
				if (customMessage == null)
				{
					java.lang.StringBuilder message = new java.lang.StringBuilder(key);
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

			internal virtual bool getAndSetAccessed()
			{
				return accessed.getAndSet(true);
			}

			public virtual void clearAccessed()
			{
				accessed.set(false);
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
				com.google.common.@base.Preconditions.checkNotNull(key);
				com.google.common.@base.Preconditions.checkNotNull(newKeys);
				com.google.common.@base.Preconditions.checkArgument(newKeys.Length > 0);
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

			public virtual string getKey()
			{
				return key;
			}

			public virtual string[] getNewKeys()
			{
				return newKeys;
			}

			public virtual string getCustomMessage()
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
			private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
				> deprecatedKeyMap;

			/// <summary>Stores a mapping from superseding keys to the keys which they deprecate.
			/// 	</summary>
			private readonly System.Collections.Generic.IDictionary<string, string> reverseDeprecatedKeyMap;

			/// <summary>
			/// Create a new DeprecationContext by copying a previous DeprecationContext
			/// and adding some deltas.
			/// </summary>
			/// <param name="other">
			/// The previous deprecation context to copy, or null to start
			/// from nothing.
			/// </param>
			/// <param name="deltas">The deltas to apply.</param>
			internal DeprecationContext(org.apache.hadoop.conf.Configuration.DeprecationContext
				 other, org.apache.hadoop.conf.Configuration.DeprecationDelta[] deltas)
			{
				System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
					> newDeprecatedKeyMap = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
					>();
				System.Collections.Generic.Dictionary<string, string> newReverseDeprecatedKeyMap = 
					new System.Collections.Generic.Dictionary<string, string>();
				if (other != null)
				{
					foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
						> entry in other.deprecatedKeyMap)
					{
						newDeprecatedKeyMap[entry.Key] = entry.Value;
					}
					foreach (System.Collections.Generic.KeyValuePair<string, string> entry_1 in other
						.reverseDeprecatedKeyMap)
					{
						newReverseDeprecatedKeyMap[entry_1.Key] = entry_1.Value;
					}
				}
				foreach (org.apache.hadoop.conf.Configuration.DeprecationDelta delta in deltas)
				{
					if (!newDeprecatedKeyMap.Contains(delta.getKey()))
					{
						org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo newKeyInfo = new org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
							(delta.getNewKeys(), delta.getCustomMessage());
						newDeprecatedKeyMap[delta.key] = newKeyInfo;
						foreach (string newKey in delta.getNewKeys())
						{
							newReverseDeprecatedKeyMap[newKey] = delta.key;
						}
					}
				}
				this.deprecatedKeyMap = org.apache.commons.collections.map.UnmodifiableMap.decorate
					(newDeprecatedKeyMap);
				this.reverseDeprecatedKeyMap = org.apache.commons.collections.map.UnmodifiableMap
					.decorate(newReverseDeprecatedKeyMap);
			}

			internal virtual System.Collections.Generic.IDictionary<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
				> getDeprecatedKeyMap()
			{
				return deprecatedKeyMap;
			}

			internal virtual System.Collections.Generic.IDictionary<string, string> getReverseDeprecatedKeyMap
				()
			{
				return reverseDeprecatedKeyMap;
			}
		}

		private static org.apache.hadoop.conf.Configuration.DeprecationDelta[] defaultDeprecations
			 = new org.apache.hadoop.conf.Configuration.DeprecationDelta[] { new org.apache.hadoop.conf.Configuration.DeprecationDelta
			("topology.script.file.name", org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("topology.script.number.args"
			, org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("hadoop.configured.node.mapping"
			, org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("topology.node.switch.mapping.impl"
			, org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("dfs.df.interval", 
			org.apache.hadoop.fs.CommonConfigurationKeys.FS_DF_INTERVAL_KEY), new org.apache.hadoop.conf.Configuration.DeprecationDelta
			("hadoop.native.lib", org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("fs.default.name", 
			org.apache.hadoop.fs.CommonConfigurationKeys.FS_DEFAULT_NAME_KEY), new org.apache.hadoop.conf.Configuration.DeprecationDelta
			("dfs.umaskmode", org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY
			), new org.apache.hadoop.conf.Configuration.DeprecationDelta("dfs.nfs.exports.allowed.hosts"
			, org.apache.hadoop.fs.CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY) };

		/// <summary>The global DeprecationContext.</summary>
		private static java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.conf.Configuration.DeprecationContext
			> deprecationContext = new java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.conf.Configuration.DeprecationContext
			>(new org.apache.hadoop.conf.Configuration.DeprecationContext(null, defaultDeprecations
			));

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
		public static void addDeprecations(org.apache.hadoop.conf.Configuration.DeprecationDelta
			[] deltas)
		{
			org.apache.hadoop.conf.Configuration.DeprecationContext prev;
			org.apache.hadoop.conf.Configuration.DeprecationContext next;
			do
			{
				prev = deprecationContext.get();
				next = new org.apache.hadoop.conf.Configuration.DeprecationContext(prev, deltas);
			}
			while (!deprecationContext.compareAndSet(prev, next));
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
		[System.ObsoleteAttribute(@"use addDeprecation(string, string, string) instead")]
		public static void addDeprecation(string key, string[] newKeys, string customMessage
			)
		{
			addDeprecations(new org.apache.hadoop.conf.Configuration.DeprecationDelta[] { new 
				org.apache.hadoop.conf.Configuration.DeprecationDelta(key, newKeys, customMessage
				) });
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
		public static void addDeprecation(string key, string newKey, string customMessage
			)
		{
			addDeprecation(key, new string[] { newKey }, customMessage);
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
		[System.ObsoleteAttribute(@"use addDeprecation(string, string) instead")]
		public static void addDeprecation(string key, string[] newKeys)
		{
			addDeprecation(key, newKeys, null);
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
		public static void addDeprecation(string key, string newKey)
		{
			addDeprecation(key, new string[] { newKey }, null);
		}

		/// <summary>checks whether the given <code>key</code> is deprecated.</summary>
		/// <param name="key">the parameter which is to be checked for deprecation</param>
		/// <returns>
		/// <code>true</code> if the key is deprecated and
		/// <code>false</code> otherwise.
		/// </returns>
		public static bool isDeprecated(string key)
		{
			return deprecationContext.get().getDeprecatedKeyMap().Contains(key);
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
		public virtual void setDeprecatedProperties()
		{
			org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
				.get();
			java.util.Properties props = getProps();
			java.util.Properties overlay = getOverlay();
			foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
				> entry in deprecations.getDeprecatedKeyMap())
			{
				string depKey = entry.Key;
				if (!overlay.contains(depKey))
				{
					foreach (string newKey in entry.Value.newKeys)
					{
						string val = overlay.getProperty(newKey);
						if (val != null)
						{
							props.setProperty(depKey, val);
							overlay.setProperty(depKey, val);
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
		private string[] handleDeprecation(org.apache.hadoop.conf.Configuration.DeprecationContext
			 deprecations, string name)
		{
			if (null != name)
			{
				name = name.Trim();
			}
			System.Collections.Generic.List<string> names = new System.Collections.Generic.List
				<string>();
			if (isDeprecated(name))
			{
				org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap
					()[name];
				warnOnceIfDeprecated(deprecations, name);
				foreach (string newKey in keyInfo.newKeys)
				{
					if (newKey != null)
					{
						names.add(newKey);
					}
				}
			}
			if (names.Count == 0)
			{
				names.add(name);
			}
			foreach (string n in names)
			{
				string deprecatedKey = deprecations.getReverseDeprecatedKeyMap()[n];
				if (deprecatedKey != null && !getOverlay().Contains(n) && getOverlay().Contains(deprecatedKey
					))
				{
					getProps().setProperty(n, getOverlay().getProperty(deprecatedKey));
					getOverlay().setProperty(n, getOverlay().getProperty(deprecatedKey));
				}
			}
			return Sharpen.Collections.ToArray(names, new string[names.Count]);
		}

		private void handleDeprecation()
		{
			LOG.debug("Handling deprecation for all properties in config...");
			org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
				.get();
			System.Collections.Generic.ICollection<object> keys = new java.util.HashSet<object
				>();
			Sharpen.Collections.AddAll(keys, getProps().Keys);
			foreach (object item in keys)
			{
				LOG.debug("Handling deprecation for " + (string)item);
				handleDeprecation(deprecations, (string)item);
			}
		}

		static Configuration()
		{
			//print deprecation warning if hadoop-site.xml is found in classpath
			java.lang.ClassLoader cL = java.lang.Thread.currentThread().getContextClassLoader
				();
			if (cL == null)
			{
				cL = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration)
					).getClassLoader();
			}
			if (cL.getResource("hadoop-site.xml") != null)
			{
				LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. " + "Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
					 + "mapred-site.xml and hdfs-site.xml to override properties of " + "core-default.xml, mapred-default.xml and hdfs-default.xml "
					 + "respectively");
			}
			addDefaultResource("core-default.xml");
			addDefaultResource("core-site.xml");
		}

		private java.util.Properties properties;

		private java.util.Properties overlay;

		private java.lang.ClassLoader classLoader;

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
				classLoader = java.lang.Thread.currentThread().getContextClassLoader();
				if (classLoader == null)
				{
					classLoader = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration
						)).getClassLoader();
				}
			}
			this.loadDefaults = loadDefaults;
			updatingResource = new java.util.concurrent.ConcurrentHashMap<string, string[]>();
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration
				)))
			{
				REGISTRY[this] = null;
			}
		}

		/// <summary>A new configuration with the same settings cloned from another.</summary>
		/// <param name="other">the configuration from which to clone settings.</param>
		public Configuration(org.apache.hadoop.conf.Configuration other)
		{
			{
				classLoader = java.lang.Thread.currentThread().getContextClassLoader();
				if (classLoader == null)
				{
					classLoader = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration
						)).getClassLoader();
				}
			}
			this.resources = (System.Collections.Generic.List<org.apache.hadoop.conf.Configuration.Resource
				>)other.resources.clone();
			lock (other)
			{
				if (other.properties != null)
				{
					this.properties = (java.util.Properties)other.properties.clone();
				}
				if (other.overlay != null)
				{
					this.overlay = (java.util.Properties)other.overlay.clone();
				}
				this.updatingResource = new java.util.concurrent.ConcurrentHashMap<string, string
					[]>(other.updatingResource);
				this.finalParameters = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap
					<string, bool>());
				Sharpen.Collections.AddAll(this.finalParameters, other.finalParameters);
			}
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.Configuration
				)))
			{
				REGISTRY[this] = null;
			}
			this.classLoader = other.classLoader;
			this.loadDefaults = other.loadDefaults;
			setQuietMode(other.getQuietMode());
		}

		/// <summary>Add a default resource.</summary>
		/// <remarks>
		/// Add a default resource. Resources are loaded in the order of the resources
		/// added.
		/// </remarks>
		/// <param name="name">file name. File should be present in the classpath.</param>
		public static void addDefaultResource(string name)
		{
			lock (typeof(Configuration))
			{
				if (!defaultResources.contains(name))
				{
					defaultResources.add(name);
					foreach (org.apache.hadoop.conf.Configuration conf in REGISTRY.Keys)
					{
						if (conf.loadDefaults)
						{
							conf.reloadConfiguration();
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
		public virtual void addResource(string name)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(name));
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
		public virtual void addResource(java.net.URL url)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(url));
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
		public virtual void addResource(org.apache.hadoop.fs.Path file)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(file));
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
		public virtual void addResource(java.io.InputStream @in)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(@in));
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
		public virtual void addResource(java.io.InputStream @in, string name)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(@in, name));
		}

		/// <summary>Add a configuration resource.</summary>
		/// <remarks>
		/// Add a configuration resource.
		/// The properties of this resource will override properties of previously
		/// added resources, unless they were marked <a href="#Final">final</a>.
		/// </remarks>
		/// <param name="conf">Configuration object from which to load properties</param>
		public virtual void addResource(org.apache.hadoop.conf.Configuration conf)
		{
			addResourceObject(new org.apache.hadoop.conf.Configuration.Resource(conf.getProps
				()));
		}

		/// <summary>Reload configuration from previously added resources.</summary>
		/// <remarks>
		/// Reload configuration from previously added resources.
		/// This method will clear all the configuration read from the added
		/// resources, and final parameters. This will make the resources to
		/// be read again before accessing the values. Values that are added
		/// via set methods will overlay values read from the resources.
		/// </remarks>
		public virtual void reloadConfiguration()
		{
			lock (this)
			{
				properties = null;
				// trigger reload
				finalParameters.clear();
			}
		}

		// clear site-limits
		private void addResourceObject(org.apache.hadoop.conf.Configuration.Resource resource
			)
		{
			lock (this)
			{
				resources.add(resource);
				// add to resources
				reloadConfiguration();
			}
		}

		private const int MAX_SUBST = 20;

		private const int SUB_START_IDX = 0;

		private const int SUB_END_IDX = SUB_START_IDX + 1;

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
		private static int[] findSubVariable(string eval)
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
									result[SUB_START_IDX] = subStart;
									result[SUB_END_IDX] = subStart + matchedLen;
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
		/// <see cref="MAX_SUBST"/>
		/// replacements are required
		/// </exception>
		private string substituteVars(string expr)
		{
			if (expr == null)
			{
				return null;
			}
			string eval = expr;
			for (int s = 0; s < MAX_SUBST; s++)
			{
				int[] varBounds = findSubVariable(eval);
				if (varBounds[SUB_START_IDX] == -1)
				{
					return eval;
				}
				string var = Sharpen.Runtime.substring(eval, varBounds[SUB_START_IDX], varBounds[
					SUB_END_IDX]);
				string val = null;
				try
				{
					val = Sharpen.Runtime.getProperty(var);
				}
				catch (System.Security.SecurityException se)
				{
					LOG.warn("Unexpected SecurityException in Configuration", se);
				}
				if (val == null)
				{
					val = getRaw(var);
				}
				if (val == null)
				{
					return eval;
				}
				// return literal ${var}: var is unbound
				int dollar = varBounds[SUB_START_IDX] - "${".Length;
				int afterRightBrace = varBounds[SUB_END_IDX] + "}".Length;
				// substitute
				eval = Sharpen.Runtime.substring(eval, 0, dollar) + val + Sharpen.Runtime.substring
					(eval, afterRightBrace);
			}
			throw new System.InvalidOperationException("Variable substitution depth too large: "
				 + MAX_SUBST + " " + expr);
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
		public virtual string get(string name)
		{
			string[] names = handleDeprecation(deprecationContext.get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = substituteVars(getProps().getProperty(n));
			}
			return result;
		}

		/// <summary>Set Configuration to allow keys without values during setup.</summary>
		/// <remarks>
		/// Set Configuration to allow keys without values during setup.  Intended
		/// for use during testing.
		/// </remarks>
		/// <param name="val">If true, will allow Configuration to store keys without values</param>
		[com.google.common.annotations.VisibleForTesting]
		public virtual void setAllowNullValueProperties(bool val)
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
		[com.google.common.annotations.VisibleForTesting]
		public virtual bool onlyKeyExists(string name)
		{
			string[] names = handleDeprecation(deprecationContext.get(), name);
			foreach (string n in names)
			{
				if (getProps().getProperty(n, DEFAULT_STRING_CHECK).Equals(DEFAULT_STRING_CHECK))
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
		public virtual string getTrimmed(string name)
		{
			string value = get(name);
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
		public virtual string getTrimmed(string name, string defaultValue)
		{
			string ret = getTrimmed(name);
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
		public virtual string getRaw(string name)
		{
			string[] names = handleDeprecation(deprecationContext.get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = getProps().getProperty(n);
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
		private string[] getAlternativeNames(string name)
		{
			string[] altNames = null;
			org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo keyInfo = null;
			org.apache.hadoop.conf.Configuration.DeprecationContext cur = deprecationContext.
				get();
			string depKey = cur.getReverseDeprecatedKeyMap()[name];
			if (depKey != null)
			{
				keyInfo = cur.getDeprecatedKeyMap()[depKey];
				if (keyInfo.newKeys.Length > 0)
				{
					if (getProps().Contains(depKey))
					{
						//if deprecated key is previously set explicitly
						System.Collections.Generic.IList<string> list = new System.Collections.Generic.List
							<string>();
						Sharpen.Collections.AddAll(list, java.util.Arrays.asList(keyInfo.newKeys));
						list.add(depKey);
						altNames = Sharpen.Collections.ToArray(list, new string[list.Count]);
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
		public virtual void set(string name, string value)
		{
			set(name, value, null);
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
		public virtual void set(string name, string value, string source)
		{
			com.google.common.@base.Preconditions.checkArgument(name != null, "Property name must not be null"
				);
			com.google.common.@base.Preconditions.checkArgument(value != null, "The value of property "
				 + name + " must not be null");
			name = name.Trim();
			org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
				.get();
			if (deprecations.getDeprecatedKeyMap().isEmpty())
			{
				getProps();
			}
			getOverlay().setProperty(name, value);
			getProps().setProperty(name, value);
			string newSource = (source == null ? "programatically" : source);
			if (!isDeprecated(name))
			{
				updatingResource[name] = new string[] { newSource };
				string[] altNames = getAlternativeNames(name);
				if (altNames != null)
				{
					foreach (string n in altNames)
					{
						if (!n.Equals(name))
						{
							getOverlay().setProperty(n, value);
							getProps().setProperty(n, value);
							updatingResource[n] = new string[] { newSource };
						}
					}
				}
			}
			else
			{
				string[] names = handleDeprecation(deprecationContext.get(), name);
				string altSource = "because " + name + " is deprecated";
				foreach (string n in names)
				{
					getOverlay().setProperty(n, value);
					getProps().setProperty(n, value);
					updatingResource[n] = new string[] { altSource };
				}
			}
		}

		private void warnOnceIfDeprecated(org.apache.hadoop.conf.Configuration.DeprecationContext
			 deprecations, string name)
		{
			org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap
				()[name];
			if (keyInfo != null && !keyInfo.getAndSetAccessed())
			{
				LOG_DEPRECATION.info(keyInfo.getWarningMessage(name));
			}
		}

		/// <summary>Unset a previously set property.</summary>
		public virtual void unset(string name)
		{
			lock (this)
			{
				string[] names = null;
				if (!isDeprecated(name))
				{
					names = getAlternativeNames(name);
					if (names == null)
					{
						names = new string[] { name };
					}
				}
				else
				{
					names = handleDeprecation(deprecationContext.get(), name);
				}
				foreach (string n in names)
				{
					getOverlay().remove(n);
					getProps().remove(n);
				}
			}
		}

		/// <summary>Sets a property if it is currently unset.</summary>
		/// <param name="name">the property name</param>
		/// <param name="value">the new value</param>
		public virtual void setIfUnset(string name, string value)
		{
			lock (this)
			{
				if (get(name) == null)
				{
					set(name, value);
				}
			}
		}

		private java.util.Properties getOverlay()
		{
			lock (this)
			{
				if (overlay == null)
				{
					overlay = new java.util.Properties();
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
		public virtual string get(string name, string defaultValue)
		{
			string[] names = handleDeprecation(deprecationContext.get(), name);
			string result = null;
			foreach (string n in names)
			{
				result = substituteVars(getProps().getProperty(n, defaultValue));
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
		/// <exception cref="java.lang.NumberFormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as an <code>int</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual int getInt(string name, int defaultValue)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			string hexString = getHexDigits(valueString);
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
		public virtual int[] getInts(string name)
		{
			string[] strings = getTrimmedStrings(name);
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
		public virtual void setInt(string name, int value)
		{
			set(name, int.toString(value));
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
		/// <exception cref="java.lang.NumberFormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>long</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual long getLong(string name, long defaultValue)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			string hexString = getHexDigits(valueString);
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
		/// <exception cref="java.lang.NumberFormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>long</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual long getLongBytes(string name, long defaultValue)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.string2long(valueString
				);
		}

		private string getHexDigits(string value)
		{
			bool negative = false;
			string str = value;
			string hexString = null;
			if (value.StartsWith("-"))
			{
				negative = true;
				str = Sharpen.Runtime.substring(value, 1);
			}
			if (str.StartsWith("0x") || str.StartsWith("0X"))
			{
				hexString = Sharpen.Runtime.substring(str, 2);
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
		public virtual void setLong(string name, long value)
		{
			set(name, System.Convert.ToString(value));
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
		/// <exception cref="java.lang.NumberFormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>float</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual float getFloat(string name, float defaultValue)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return float.parseFloat(valueString);
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>float</code>.
		/// 	</summary>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		public virtual void setFloat(string name, float value)
		{
			set(name, float.toString(value));
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
		/// <exception cref="java.lang.NumberFormatException">when the value is invalid</exception>
		/// <returns>
		/// property value as a <code>double</code>,
		/// or <code>defaultValue</code>.
		/// </returns>
		public virtual double getDouble(string name, double defaultValue)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			return double.parseDouble(valueString);
		}

		/// <summary>Set the value of the <code>name</code> property to a <code>double</code>.
		/// 	</summary>
		/// <param name="name">property name.</param>
		/// <param name="value">property value.</param>
		public virtual void setDouble(string name, double value)
		{
			set(name, double.toString(value));
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
		public virtual bool getBoolean(string name, bool defaultValue)
		{
			string valueString = getTrimmed(name);
			if (null == valueString || valueString.isEmpty())
			{
				return defaultValue;
			}
			if (org.apache.hadoop.util.StringUtils.equalsIgnoreCase("true", valueString))
			{
				return true;
			}
			else
			{
				if (org.apache.hadoop.util.StringUtils.equalsIgnoreCase("false", valueString))
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
		public virtual void setBoolean(string name, bool value)
		{
			set(name, bool.toString(value));
		}

		/// <summary>Set the given property, if it is currently unset.</summary>
		/// <param name="name">property name</param>
		/// <param name="value">new value</param>
		public virtual void setBooleanIfUnset(string name, bool value)
		{
			setIfUnset(name, bool.toString(value));
		}

		/// <summary>Set the value of the <code>name</code> property to the given type.</summary>
		/// <remarks>
		/// Set the value of the <code>name</code> property to the given type. This
		/// is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
		/// </remarks>
		/// <param name="name">property name</param>
		/// <param name="value">new value</param>
		public virtual void setEnum<T>(string name, T value)
			where T : java.lang.Enum<T>
		{
			set(name, value.ToString());
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
		public virtual T getEnum<T>(string name, T defaultValue)
			where T : java.lang.Enum<T>
		{
			string val = getTrimmed(name);
			return null == val ? defaultValue : java.lang.Enum.valueOf(defaultValue.getDeclaringClass
				(), val);
		}

		[System.Serializable]
		internal sealed class ParsedTimeDuration
		{
			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration NS
				 = new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration US
				 = new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration MS
				 = new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration S = 
				new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration M = 
				new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration H = 
				new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			public static readonly org.apache.hadoop.conf.Configuration.ParsedTimeDuration D = 
				new org.apache.hadoop.conf.Configuration.ParsedTimeDuration();

			internal abstract java.util.concurrent.TimeUnit unit();

			internal abstract string suffix();

			internal static org.apache.hadoop.conf.Configuration.ParsedTimeDuration unitFor(string
				 s)
			{
				foreach (org.apache.hadoop.conf.Configuration.ParsedTimeDuration ptd in values())
				{
					// iteration order is in decl order, so SECONDS matched last
					if (s.EndsWith(ptd.suffix()))
					{
						return ptd;
					}
				}
				return null;
			}

			internal static org.apache.hadoop.conf.Configuration.ParsedTimeDuration unitFor(java.util.concurrent.TimeUnit
				 unit)
			{
				foreach (org.apache.hadoop.conf.Configuration.ParsedTimeDuration ptd in values())
				{
					if (ptd.unit() == unit)
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
		public virtual void setTimeDuration(string name, long value, java.util.concurrent.TimeUnit
			 unit)
		{
			set(name, value + org.apache.hadoop.conf.Configuration.ParsedTimeDuration.unitFor
				(unit).suffix());
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
		/// <exception cref="java.lang.NumberFormatException">
		/// If the property stripped of its unit is not
		/// a number
		/// </exception>
		public virtual long getTimeDuration(string name, long defaultValue, java.util.concurrent.TimeUnit
			 unit)
		{
			string vStr = get(name);
			if (null == vStr)
			{
				return defaultValue;
			}
			vStr = vStr.Trim();
			org.apache.hadoop.conf.Configuration.ParsedTimeDuration vUnit = org.apache.hadoop.conf.Configuration.ParsedTimeDuration
				.unitFor(vStr);
			if (null == vUnit)
			{
				LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
				vUnit = org.apache.hadoop.conf.Configuration.ParsedTimeDuration.unitFor(unit);
			}
			else
			{
				vStr = Sharpen.Runtime.substring(vStr, 0, vStr.LastIndexOf(vUnit.suffix()));
			}
			return unit.convert(long.Parse(vStr), vUnit.unit());
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
		public virtual java.util.regex.Pattern getPattern(string name, java.util.regex.Pattern
			 defaultValue)
		{
			string valString = get(name);
			if (null == valString || valString.isEmpty())
			{
				return defaultValue;
			}
			try
			{
				return java.util.regex.Pattern.compile(valString);
			}
			catch (java.util.regex.PatternSyntaxException pse)
			{
				LOG.warn("Regular expression '" + valString + "' for property '" + name + "' not valid. Using default"
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
		public virtual void setPattern(string name, java.util.regex.Pattern pattern)
		{
			System.Diagnostics.Debug.Assert(pattern != null, "Pattern cannot be null");
			set(name, pattern.pattern());
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
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		public virtual string[] getPropertySources(string name)
		{
			lock (this)
			{
				if (properties == null)
				{
					// If properties is null, it means a resource was newly added
					// but the props were cleared so as to load it upon future
					// requests. So lets force a load by asking a properties list.
					getProps();
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
						return java.util.Arrays.copyOf(source, source.Length);
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
		public class IntegerRanges : System.Collections.Generic.IEnumerable<int>
		{
			private class Range
			{
				internal int start;

				internal int end;
			}

			private class RangeNumberIterator : System.Collections.Generic.IEnumerator<int>
			{
				internal System.Collections.Generic.IEnumerator<org.apache.hadoop.conf.Configuration.IntegerRanges.Range
					> @internal;

				internal int at;

				internal int end;

				public RangeNumberIterator(System.Collections.Generic.IList<org.apache.hadoop.conf.Configuration.IntegerRanges.Range
					> ranges)
				{
					if (ranges != null)
					{
						@internal = ranges.GetEnumerator();
					}
					at = -1;
					end = -2;
				}

				public override bool MoveNext()
				{
					if (at <= end)
					{
						return true;
					}
					else
					{
						if (@internal != null)
						{
							return @internal.MoveNext();
						}
					}
					return false;
				}

				public override int Current
				{
					get
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
								org.apache.hadoop.conf.Configuration.IntegerRanges.Range found = @internal.Current;
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
				}

				public override void remove()
				{
					throw new System.NotSupportedException();
				}
			}

			internal System.Collections.Generic.IList<org.apache.hadoop.conf.Configuration.IntegerRanges.Range
				> ranges = new System.Collections.Generic.List<org.apache.hadoop.conf.Configuration.IntegerRanges.Range
				>();

			public IntegerRanges()
			{
			}

			public IntegerRanges(string newValue)
			{
				java.util.StringTokenizer itr = new java.util.StringTokenizer(newValue, ",");
				while (itr.hasMoreTokens())
				{
					string rng = itr.nextToken().Trim();
					string[] parts = rng.split("-", 3);
					if (parts.Length < 1 || parts.Length > 2)
					{
						throw new System.ArgumentException("integer range badly formed: " + rng);
					}
					org.apache.hadoop.conf.Configuration.IntegerRanges.Range r = new org.apache.hadoop.conf.Configuration.IntegerRanges.Range
						();
					r.start = convertToInt(parts[0], 0);
					if (parts.Length == 2)
					{
						r.end = convertToInt(parts[1], int.MaxValue);
					}
					else
					{
						r.end = r.start;
					}
					if (r.start > r.end)
					{
						throw new System.ArgumentException("IntegerRange from " + r.start + " to " + r.end
							 + " is invalid");
					}
					ranges.add(r);
				}
			}

			/// <summary>Convert a string to an int treating empty strings as the default value.</summary>
			/// <param name="value">the string value</param>
			/// <param name="defaultValue">the value for if the string is empty</param>
			/// <returns>the desired integer</returns>
			private static int convertToInt(string value, int defaultValue)
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
			public virtual bool isIncluded(int value)
			{
				foreach (org.apache.hadoop.conf.Configuration.IntegerRanges.Range r in ranges)
				{
					if (r.start <= value && value <= r.end)
					{
						return true;
					}
				}
				return false;
			}

			/// <returns>true if there are no values in this range, else false.</returns>
			public virtual bool isEmpty()
			{
				return ranges == null || ranges.isEmpty();
			}

			public override string ToString()
			{
				java.lang.StringBuilder result = new java.lang.StringBuilder();
				bool first = true;
				foreach (org.apache.hadoop.conf.Configuration.IntegerRanges.Range r in ranges)
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

			public override System.Collections.Generic.IEnumerator<int> GetEnumerator()
			{
				return new org.apache.hadoop.conf.Configuration.IntegerRanges.RangeNumberIterator
					(ranges);
			}
		}

		/// <summary>Parse the given attribute as a set of integer ranges</summary>
		/// <param name="name">the attribute name</param>
		/// <param name="defaultValue">the default value if it is not set</param>
		/// <returns>a new set of ranges from the configured value</returns>
		public virtual org.apache.hadoop.conf.Configuration.IntegerRanges getRange(string
			 name, string defaultValue)
		{
			return new org.apache.hadoop.conf.Configuration.IntegerRanges(get(name, defaultValue
				));
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
		/// <see cref="getStrings(string)"/>
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <returns>property value as a collection of <code>String</code>s.</returns>
		public virtual System.Collections.Generic.ICollection<string> getStringCollection
			(string name)
		{
			string valueString = get(name);
			return org.apache.hadoop.util.StringUtils.getStringCollection(valueString);
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
		public virtual string[] getStrings(string name)
		{
			string valueString = get(name);
			return org.apache.hadoop.util.StringUtils.getStrings(valueString);
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
		public virtual string[] getStrings(string name, params string[] defaultValue)
		{
			string valueString = get(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			else
			{
				return org.apache.hadoop.util.StringUtils.getStrings(valueString);
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
		public virtual System.Collections.Generic.ICollection<string> getTrimmedStringCollection
			(string name)
		{
			string valueString = get(name);
			if (null == valueString)
			{
				System.Collections.Generic.ICollection<string> empty = new System.Collections.Generic.List
					<string>();
				return empty;
			}
			return org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(valueString);
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
		public virtual string[] getTrimmedStrings(string name)
		{
			string valueString = get(name);
			return org.apache.hadoop.util.StringUtils.getTrimmedStrings(valueString);
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
		public virtual string[] getTrimmedStrings(string name, params string[] defaultValue
			)
		{
			string valueString = get(name);
			if (null == valueString)
			{
				return defaultValue;
			}
			else
			{
				return org.apache.hadoop.util.StringUtils.getTrimmedStrings(valueString);
			}
		}

		/// <summary>
		/// Set the array of string values for the <code>name</code> property as
		/// as comma delimited values.
		/// </summary>
		/// <param name="name">property name.</param>
		/// <param name="values">The values</param>
		public virtual void setStrings(string name, params string[] values)
		{
			set(name, org.apache.hadoop.util.StringUtils.arrayToString(values));
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
		public virtual char[] getPassword(string name)
		{
			char[] pass = null;
			pass = getPasswordFromCredentialProviders(name);
			if (pass == null)
			{
				pass = getPasswordFromConfig(name);
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
		protected internal virtual char[] getPasswordFromCredentialProviders(string name)
		{
			char[] pass = null;
			try
			{
				System.Collections.Generic.IList<org.apache.hadoop.security.alias.CredentialProvider
					> providers = org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders
					(this);
				if (providers != null)
				{
					foreach (org.apache.hadoop.security.alias.CredentialProvider provider in providers)
					{
						try
						{
							org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry entry = provider
								.getCredentialEntry(name);
							if (entry != null)
							{
								pass = entry.getCredential();
								break;
							}
						}
						catch (System.IO.IOException ioe)
						{
							throw new System.IO.IOException("Can't get key " + name + " from key provider" + 
								"of type: " + Sharpen.Runtime.getClassForObject(provider).getName() + ".", ioe);
						}
					}
				}
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.IO.IOException("Configuration problem with provider path.", ioe);
			}
			return pass;
		}

		/// <summary>Fallback to clear text passwords in configuration.</summary>
		/// <param name="name"/>
		/// <returns>clear text password or null</returns>
		protected internal virtual char[] getPasswordFromConfig(string name)
		{
			char[] pass = null;
			if (getBoolean(org.apache.hadoop.security.alias.CredentialProvider.CLEAR_TEXT_FALLBACK
				, true))
			{
				string passStr = get(name);
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
		public virtual java.net.InetSocketAddress getSocketAddr(string hostProperty, string
			 addressProperty, string defaultAddressValue, int defaultPort)
		{
			java.net.InetSocketAddress bindAddr = getSocketAddr(addressProperty, defaultAddressValue
				, defaultPort);
			string host = get(hostProperty);
			if (host == null || host.isEmpty())
			{
				return bindAddr;
			}
			return org.apache.hadoop.net.NetUtils.createSocketAddr(host, bindAddr.getPort(), 
				hostProperty);
		}

		/// <summary>
		/// Get the socket address for <code>name</code> property as a
		/// <code>InetSocketAddress</code>.
		/// </summary>
		/// <param name="name">property name.</param>
		/// <param name="defaultAddress">the default value</param>
		/// <param name="defaultPort">the default port</param>
		/// <returns>InetSocketAddress</returns>
		public virtual java.net.InetSocketAddress getSocketAddr(string name, string defaultAddress
			, int defaultPort)
		{
			string address = getTrimmed(name, defaultAddress);
			return org.apache.hadoop.net.NetUtils.createSocketAddr(address, defaultPort, name
				);
		}

		/// <summary>
		/// Set the socket address for the <code>name</code> property as
		/// a <code>host:port</code>.
		/// </summary>
		public virtual void setSocketAddr(string name, java.net.InetSocketAddress addr)
		{
			set(name, org.apache.hadoop.net.NetUtils.getHostPortString(addr));
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
		public virtual java.net.InetSocketAddress updateConnectAddr(string hostProperty, 
			string addressProperty, string defaultAddressValue, java.net.InetSocketAddress addr
			)
		{
			string host = get(hostProperty);
			string connectHostPort = getTrimmed(addressProperty, defaultAddressValue);
			if (host == null || host.isEmpty() || connectHostPort == null || connectHostPort.
				isEmpty())
			{
				//not our case, fall back to original logic
				return updateConnectAddr(addressProperty, addr);
			}
			string connectHost = connectHostPort.split(":")[0];
			// Create connect address using client address hostname and server port.
			return updateConnectAddr(addressProperty, org.apache.hadoop.net.NetUtils.createSocketAddrForHost
				(connectHost, addr.getPort()));
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
		public virtual java.net.InetSocketAddress updateConnectAddr(string name, java.net.InetSocketAddress
			 addr)
		{
			java.net.InetSocketAddress connectAddr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(addr);
			setSocketAddr(name, connectAddr);
			return connectAddr;
		}

		/// <summary>Load a class by name.</summary>
		/// <param name="name">the class name.</param>
		/// <returns>the class object.</returns>
		/// <exception cref="java.lang.ClassNotFoundException">if the class is not found.</exception>
		public virtual java.lang.Class getClassByName(string name)
		{
			java.lang.Class ret = getClassByNameOrNull(name);
			if (ret == null)
			{
				throw new java.lang.ClassNotFoundException("Class " + name + " not found");
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
		public virtual java.lang.Class getClassByNameOrNull(string name)
		{
			System.Collections.Generic.IDictionary<string, java.lang.@ref.WeakReference<java.lang.Class
				>> map;
			lock (CACHE_CLASSES)
			{
				map = CACHE_CLASSES[classLoader];
				if (map == null)
				{
					map = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<string, java.lang.@ref.WeakReference
						<java.lang.Class>>());
					CACHE_CLASSES[classLoader] = map;
				}
			}
			java.lang.Class clazz = null;
			java.lang.@ref.WeakReference<java.lang.Class> @ref = map[name];
			if (@ref != null)
			{
				clazz = @ref.get();
			}
			if (clazz == null)
			{
				try
				{
					clazz = java.lang.Class.forName(name, true, classLoader);
				}
				catch (java.lang.ClassNotFoundException)
				{
					// Leave a marker that the class isn't found
					map[name] = new java.lang.@ref.WeakReference<java.lang.Class>(NEGATIVE_CACHE_SENTINEL
						);
					return null;
				}
				// two putters can race here, but they'll put the same class
				map[name] = new java.lang.@ref.WeakReference<java.lang.Class>(clazz);
				return clazz;
			}
			else
			{
				if (clazz == NEGATIVE_CACHE_SENTINEL)
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
		public virtual java.lang.Class[] getClasses(string name, params java.lang.Class[]
			 defaultValue)
		{
			string[] classnames = getTrimmedStrings(name);
			if (classnames == null)
			{
				return defaultValue;
			}
			try
			{
				java.lang.Class[] classes = new java.lang.Class[classnames.Length];
				for (int i = 0; i < classnames.Length; i++)
				{
					classes[i] = getClassByName(classnames[i]);
				}
				return classes;
			}
			catch (java.lang.ClassNotFoundException e)
			{
				throw new System.Exception(e);
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
		public virtual java.lang.Class getClass(string name, java.lang.Class defaultValue
			)
		{
			string valueString = getTrimmed(name);
			if (valueString == null)
			{
				return defaultValue;
			}
			try
			{
				return getClassByName(valueString);
			}
			catch (java.lang.ClassNotFoundException e)
			{
				throw new System.Exception(e);
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
		public virtual java.lang.Class getClass<U>(string name, java.lang.Class defaultValue
			)
		{
			System.Type xface = typeof(U);
			try
			{
				java.lang.Class theClass = getClass(name, defaultValue);
				if (theClass != null && !xface.isAssignableFrom(theClass))
				{
					throw new System.Exception(theClass + " not " + xface.getName());
				}
				else
				{
					if (theClass != null)
					{
						return theClass.asSubclass(xface);
					}
					else
					{
						return null;
					}
				}
			}
			catch (System.Exception e)
			{
				throw new System.Exception(e);
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
		public virtual System.Collections.Generic.IList<U> getInstances<U>(string name)
		{
			System.Type xface = typeof(U);
			System.Collections.Generic.IList<U> ret = new System.Collections.Generic.List<U>(
				);
			java.lang.Class[] classes = getClasses(name);
			foreach (java.lang.Class cl in classes)
			{
				if (!xface.isAssignableFrom(cl))
				{
					throw new System.Exception(cl + " does not implement " + xface);
				}
				ret.add((U)org.apache.hadoop.util.ReflectionUtils.newInstance(cl, this));
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
		public virtual void setClass(string name, java.lang.Class theClass, java.lang.Class
			 xface)
		{
			if (!xface.isAssignableFrom(theClass))
			{
				throw new System.Exception(theClass + " not " + xface.getName());
			}
			set(name, theClass.getName());
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
		public virtual org.apache.hadoop.fs.Path getLocalPath(string dirsProp, string path
			)
		{
			string[] dirs = getTrimmedStrings(dirsProp);
			int hashCode = path.GetHashCode();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(this
				);
			for (int i = 0; i < dirs.Length; i++)
			{
				// try each local dir
				int index = (hashCode + i & int.MaxValue) % dirs.Length;
				org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(dirs[index], path);
				org.apache.hadoop.fs.Path dir = file.getParent();
				if (fs.mkdirs(dir) || fs.exists(dir))
				{
					return file;
				}
			}
			LOG.warn("Could not make " + path + " in local directories from " + dirsProp);
			for (int i_1 = 0; i_1 < dirs.Length; i_1++)
			{
				int index = (hashCode + i_1 & int.MaxValue) % dirs.Length;
				LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
			}
			throw new System.IO.IOException("No valid local directories in property: " + dirsProp
				);
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
		public virtual java.io.File getFile(string dirsProp, string path)
		{
			string[] dirs = getTrimmedStrings(dirsProp);
			int hashCode = path.GetHashCode();
			for (int i = 0; i < dirs.Length; i++)
			{
				// try each local dir
				int index = (hashCode + i & int.MaxValue) % dirs.Length;
				java.io.File file = new java.io.File(dirs[index], path);
				java.io.File dir = file.getParentFile();
				if (dir.exists() || dir.mkdirs())
				{
					return file;
				}
			}
			throw new System.IO.IOException("No valid local directories in property: " + dirsProp
				);
		}

		/// <summary>
		/// Get the
		/// <see cref="java.net.URL"/>
		/// for the named resource.
		/// </summary>
		/// <param name="name">resource name.</param>
		/// <returns>the url for the named resource.</returns>
		public virtual java.net.URL getResource(string name)
		{
			return classLoader.getResource(name);
		}

		/// <summary>
		/// Get an input stream attached to the configuration resource with the
		/// given <code>name</code>.
		/// </summary>
		/// <param name="name">configuration resource name.</param>
		/// <returns>an input stream attached to the resource.</returns>
		public virtual java.io.InputStream getConfResourceAsInputStream(string name)
		{
			try
			{
				java.net.URL url = getResource(name);
				if (url == null)
				{
					LOG.info(name + " not found");
					return null;
				}
				else
				{
					LOG.info("found resource " + name + " at " + url);
				}
				return url.openStream();
			}
			catch (System.Exception)
			{
				return null;
			}
		}

		/// <summary>
		/// Get a
		/// <see cref="java.io.Reader"/>
		/// attached to the configuration resource with the
		/// given <code>name</code>.
		/// </summary>
		/// <param name="name">configuration resource name.</param>
		/// <returns>a reader attached to the resource.</returns>
		public virtual java.io.Reader getConfResourceAsReader(string name)
		{
			try
			{
				java.net.URL url = getResource(name);
				if (url == null)
				{
					LOG.info(name + " not found");
					return null;
				}
				else
				{
					LOG.info("found resource " + name + " at " + url);
				}
				return new java.io.InputStreamReader(url.openStream(), com.google.common.@base.Charsets
					.UTF_8);
			}
			catch (System.Exception)
			{
				return null;
			}
		}

		/// <summary>Get the set of parameters marked final.</summary>
		/// <returns>final parameter set.</returns>
		public virtual System.Collections.Generic.ICollection<string> getFinalParameters(
			)
		{
			System.Collections.Generic.ICollection<string> setFinalParams = java.util.Collections
				.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<string, bool>());
			Sharpen.Collections.AddAll(setFinalParams, finalParameters);
			return setFinalParams;
		}

		protected internal virtual java.util.Properties getProps()
		{
			lock (this)
			{
				if (properties == null)
				{
					properties = new java.util.Properties();
					System.Collections.Generic.IDictionary<string, string[]> backup = new java.util.concurrent.ConcurrentHashMap
						<string, string[]>(updatingResource);
					loadResources(properties, resources, quietmode);
					if (overlay != null)
					{
						properties.putAll(overlay);
						foreach (System.Collections.Generic.KeyValuePair<object, object> item in overlay)
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
		public virtual int size()
		{
			return getProps().Count;
		}

		/// <summary>Clears all keys from the configuration.</summary>
		public virtual void clear()
		{
			getProps().clear();
			getOverlay().clear();
		}

		/// <summary>
		/// Get an
		/// <see cref="System.Collections.IEnumerator{E}"/>
		/// to go through the list of <code>String</code>
		/// key-value pairs in the configuration.
		/// </summary>
		/// <returns>an iterator over the entries.</returns>
		public virtual System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
			<string, string>> GetEnumerator()
		{
			// Get a copy of just the string to string pairs. After the old object
			// methods that allow non-strings to be put into configurations are removed,
			// we could replace properties with a Map<String,String> and get rid of this
			// code.
			System.Collections.Generic.IDictionary<string, string> result = new System.Collections.Generic.Dictionary
				<string, string>();
			foreach (System.Collections.Generic.KeyValuePair<object, object> item in getProps
				())
			{
				if (item.Key is string && item.Value is string)
				{
					result[(string)item.Key] = (string)item.Value;
				}
			}
			return result.GetEnumerator();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.xml.sax.SAXException"/>
		private org.w3c.dom.Document parse(javax.xml.parsers.DocumentBuilder builder, java.net.URL
			 url)
		{
			if (!quietmode)
			{
				LOG.debug("parsing URL " + url);
			}
			if (url == null)
			{
				return null;
			}
			return parse(builder, url.openStream(), url.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.xml.sax.SAXException"/>
		private org.w3c.dom.Document parse(javax.xml.parsers.DocumentBuilder builder, java.io.InputStream
			 @is, string systemId)
		{
			if (!quietmode)
			{
				LOG.debug("parsing input stream " + @is);
			}
			if (@is == null)
			{
				return null;
			}
			try
			{
				return (systemId == null) ? builder.parse(@is) : builder.parse(@is, systemId);
			}
			finally
			{
				@is.close();
			}
		}

		private void loadResources(java.util.Properties properties, System.Collections.Generic.List
			<org.apache.hadoop.conf.Configuration.Resource> resources, bool quiet)
		{
			if (loadDefaults)
			{
				foreach (string resource in defaultResources)
				{
					loadResource(properties, new org.apache.hadoop.conf.Configuration.Resource(resource
						), quiet);
				}
				//support the hadoop-site.xml as a deprecated case
				if (getResource("hadoop-site.xml") != null)
				{
					loadResource(properties, new org.apache.hadoop.conf.Configuration.Resource("hadoop-site.xml"
						), quiet);
				}
			}
			for (int i = 0; i < resources.Count; i++)
			{
				org.apache.hadoop.conf.Configuration.Resource ret = loadResource(properties, resources
					[i], quiet);
				if (ret != null)
				{
					resources.set(i, ret);
				}
			}
		}

		private org.apache.hadoop.conf.Configuration.Resource loadResource(java.util.Properties
			 properties, org.apache.hadoop.conf.Configuration.Resource wrapper, bool quiet)
		{
			string name = UNKNOWN_RESOURCE;
			try
			{
				object resource = wrapper.getResource();
				name = wrapper.getName();
				javax.xml.parsers.DocumentBuilderFactory docBuilderFactory = javax.xml.parsers.DocumentBuilderFactory
					.newInstance();
				//ignore all comments inside the xml file
				docBuilderFactory.setIgnoringComments(true);
				//allow includes in the xml file
				docBuilderFactory.setNamespaceAware(true);
				try
				{
					docBuilderFactory.setXIncludeAware(true);
				}
				catch (System.NotSupportedException e)
				{
					LOG.error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory 
						+ ":" + e, e);
				}
				javax.xml.parsers.DocumentBuilder builder = docBuilderFactory.newDocumentBuilder(
					);
				org.w3c.dom.Document doc = null;
				org.w3c.dom.Element root = null;
				bool returnCachedProperties = false;
				if (resource is java.net.URL)
				{
					// an URL resource
					doc = parse(builder, (java.net.URL)resource);
				}
				else
				{
					if (resource is string)
					{
						// a CLASSPATH resource
						java.net.URL url = getResource((string)resource);
						doc = parse(builder, url);
					}
					else
					{
						if (resource is org.apache.hadoop.fs.Path)
						{
							// a file resource
							// Can't use FileSystem API or we get an infinite loop
							// since FileSystem uses Configuration API.  Use java.io.File instead.
							java.io.File file = new java.io.File(((org.apache.hadoop.fs.Path)resource).toUri(
								).getPath()).getAbsoluteFile();
							if (file.exists())
							{
								if (!quiet)
								{
									LOG.debug("parsing File " + file);
								}
								doc = parse(builder, new java.io.BufferedInputStream(new java.io.FileInputStream(
									file)), ((org.apache.hadoop.fs.Path)resource).ToString());
							}
						}
						else
						{
							if (resource is java.io.InputStream)
							{
								doc = parse(builder, (java.io.InputStream)resource, null);
								returnCachedProperties = true;
							}
							else
							{
								if (resource is java.util.Properties)
								{
									overlay(properties, (java.util.Properties)resource);
								}
								else
								{
									if (resource is org.w3c.dom.Element)
									{
										root = (org.w3c.dom.Element)resource;
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
						throw new System.Exception(resource + " not found");
					}
					root = doc.getDocumentElement();
				}
				java.util.Properties toAddTo = properties;
				if (returnCachedProperties)
				{
					toAddTo = new java.util.Properties();
				}
				if (!"configuration".Equals(root.getTagName()))
				{
					LOG.fatal("bad conf file: top-level element not <configuration>");
				}
				org.w3c.dom.NodeList props = root.getChildNodes();
				org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
					.get();
				for (int i = 0; i < props.getLength(); i++)
				{
					org.w3c.dom.Node propNode = props.item(i);
					if (!(propNode is org.w3c.dom.Element))
					{
						continue;
					}
					org.w3c.dom.Element prop = (org.w3c.dom.Element)propNode;
					if ("configuration".Equals(prop.getTagName()))
					{
						loadResource(toAddTo, new org.apache.hadoop.conf.Configuration.Resource(prop, name
							), quiet);
						continue;
					}
					if (!"property".Equals(prop.getTagName()))
					{
						LOG.warn("bad conf file: element not <property>");
					}
					org.w3c.dom.NodeList fields = prop.getChildNodes();
					string attr = null;
					string value = null;
					bool finalParameter = false;
					System.Collections.Generic.LinkedList<string> source = new System.Collections.Generic.LinkedList
						<string>();
					for (int j = 0; j < fields.getLength(); j++)
					{
						org.w3c.dom.Node fieldNode = fields.item(j);
						if (!(fieldNode is org.w3c.dom.Element))
						{
							continue;
						}
						org.w3c.dom.Element field = (org.w3c.dom.Element)fieldNode;
						if ("name".Equals(field.getTagName()) && field.hasChildNodes())
						{
							attr = org.apache.hadoop.util.StringInterner.weakIntern(((org.w3c.dom.Text)field.
								getFirstChild()).getData().Trim());
						}
						if ("value".Equals(field.getTagName()) && field.hasChildNodes())
						{
							value = org.apache.hadoop.util.StringInterner.weakIntern(((org.w3c.dom.Text)field
								.getFirstChild()).getData());
						}
						if ("final".Equals(field.getTagName()) && field.hasChildNodes())
						{
							finalParameter = "true".Equals(((org.w3c.dom.Text)field.getFirstChild()).getData(
								));
						}
						if ("source".Equals(field.getTagName()) && field.hasChildNodes())
						{
							source.add(org.apache.hadoop.util.StringInterner.weakIntern(((org.w3c.dom.Text)field
								.getFirstChild()).getData()));
						}
					}
					source.add(name);
					// Ignore this parameter if it has already been marked as 'final'
					if (attr != null)
					{
						if (deprecations.getDeprecatedKeyMap().Contains(attr))
						{
							org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap
								()[attr];
							keyInfo.clearAccessed();
							foreach (string key in keyInfo.newKeys)
							{
								// update new keys with deprecated key's value 
								loadProperty(toAddTo, name, key, value, finalParameter, Sharpen.Collections.ToArray
									(source, new string[source.Count]));
							}
						}
						else
						{
							loadProperty(toAddTo, name, attr, value, finalParameter, Sharpen.Collections.ToArray
								(source, new string[source.Count]));
						}
					}
				}
				if (returnCachedProperties)
				{
					overlay(properties, toAddTo);
					return new org.apache.hadoop.conf.Configuration.Resource(toAddTo, name);
				}
				return null;
			}
			catch (System.IO.IOException e)
			{
				LOG.fatal("error parsing conf " + name, e);
				throw new System.Exception(e);
			}
			catch (org.w3c.dom.DOMException e)
			{
				LOG.fatal("error parsing conf " + name, e);
				throw new System.Exception(e);
			}
			catch (org.xml.sax.SAXException e)
			{
				LOG.fatal("error parsing conf " + name, e);
				throw new System.Exception(e);
			}
			catch (javax.xml.parsers.ParserConfigurationException e)
			{
				LOG.fatal("error parsing conf " + name, e);
				throw new System.Exception(e);
			}
		}

		private void overlay(java.util.Properties to, java.util.Properties from)
		{
			foreach (System.Collections.Generic.KeyValuePair<object, object> entry in from)
			{
				to[entry.Key] = entry.Value;
			}
		}

		private void loadProperty(java.util.Properties properties, string name, string attr
			, string value, bool finalParameter, string[] source)
		{
			if (value != null || allowNullValueProperties)
			{
				if (!finalParameters.contains(attr))
				{
					if (value == null && allowNullValueProperties)
					{
						value = DEFAULT_STRING_CHECK;
					}
					properties.setProperty(attr, value);
					if (source != null)
					{
						updatingResource[attr] = source;
					}
				}
				else
				{
					if (!value.Equals(properties.getProperty(attr)))
					{
						LOG.warn(name + ":an attempt to override final parameter: " + attr + ";  Ignoring."
							);
					}
				}
			}
			if (finalParameter && attr != null)
			{
				finalParameters.add(attr);
			}
		}

		/// <summary>
		/// Write out the non-default properties in this configuration to the given
		/// <see cref="java.io.OutputStream"/>
		/// using UTF-8 encoding.
		/// </summary>
		/// <param name="out">the output stream to write to.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void writeXml(java.io.OutputStream @out)
		{
			writeXml(new java.io.OutputStreamWriter(@out, "UTF-8"));
		}

		/// <summary>
		/// Write out the non-default properties in this configuration to the given
		/// <see cref="System.IO.TextWriter"/>
		/// .
		/// </summary>
		/// <param name="out">the writer to write to.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void writeXml(System.IO.TextWriter @out)
		{
			org.w3c.dom.Document doc = asXmlDocument();
			try
			{
				javax.xml.transform.dom.DOMSource source = new javax.xml.transform.dom.DOMSource(
					doc);
				javax.xml.transform.stream.StreamResult result = new javax.xml.transform.stream.StreamResult
					(@out);
				javax.xml.transform.TransformerFactory transFactory = javax.xml.transform.TransformerFactory
					.newInstance();
				javax.xml.transform.Transformer transformer = transFactory.newTransformer();
				// Important to not hold Configuration log while writing result, since
				// 'out' may be an HDFS stream which needs to lock this configuration
				// from another thread.
				transformer.transform(source, result);
			}
			catch (javax.xml.transform.TransformerException te)
			{
				throw new System.IO.IOException(te);
			}
		}

		/// <summary>Return the XML DOM corresponding to this Configuration.</summary>
		/// <exception cref="System.IO.IOException"/>
		private org.w3c.dom.Document asXmlDocument()
		{
			lock (this)
			{
				org.w3c.dom.Document doc;
				try
				{
					doc = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder()
						.newDocument();
				}
				catch (javax.xml.parsers.ParserConfigurationException pe)
				{
					throw new System.IO.IOException(pe);
				}
				org.w3c.dom.Element conf = doc.createElement("configuration");
				doc.appendChild(conf);
				conf.appendChild(doc.createTextNode("\n"));
				handleDeprecation();
				//ensure properties is set and deprecation is handled
				for (java.util.Enumeration<object> e = properties.Keys; e.MoveNext(); )
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
					org.w3c.dom.Element propNode = doc.createElement("property");
					conf.appendChild(propNode);
					org.w3c.dom.Element nameNode = doc.createElement("name");
					nameNode.appendChild(doc.createTextNode(name));
					propNode.appendChild(nameNode);
					org.w3c.dom.Element valueNode = doc.createElement("value");
					valueNode.appendChild(doc.createTextNode(value));
					propNode.appendChild(valueNode);
					if (updatingResource != null)
					{
						string[] sources = updatingResource[name];
						if (sources != null)
						{
							foreach (string s in sources)
							{
								org.w3c.dom.Element sourceNode = doc.createElement("source");
								sourceNode.appendChild(doc.createTextNode(s));
								propNode.appendChild(sourceNode);
							}
						}
					}
					conf.appendChild(doc.createTextNode("\n"));
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
		public static void dumpConfiguration(org.apache.hadoop.conf.Configuration config, 
			System.IO.TextWriter @out)
		{
			org.codehaus.jackson.JsonFactory dumpFactory = new org.codehaus.jackson.JsonFactory
				();
			org.codehaus.jackson.JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator
				(@out);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeFieldName("properties");
			dumpGenerator.writeStartArray();
			dumpGenerator.flush();
			lock (config)
			{
				foreach (System.Collections.Generic.KeyValuePair<object, object> item in config.getProps
					())
				{
					dumpGenerator.writeStartObject();
					dumpGenerator.writeStringField("key", (string)item.Key);
					dumpGenerator.writeStringField("value", config.get((string)item.Key));
					dumpGenerator.writeBooleanField("isFinal", config.finalParameters.contains(item.Key
						));
					string[] resources = config.updatingResource[item.Key];
					string resource = UNKNOWN_RESOURCE;
					if (resources != null && resources.Length > 0)
					{
						resource = resources[0];
					}
					dumpGenerator.writeStringField("resource", resource);
					dumpGenerator.writeEndObject();
				}
			}
			dumpGenerator.writeEndArray();
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		}

		/// <summary>
		/// Get the
		/// <see cref="java.lang.ClassLoader"/>
		/// for this job.
		/// </summary>
		/// <returns>the correct class loader.</returns>
		public virtual java.lang.ClassLoader getClassLoader()
		{
			return classLoader;
		}

		/// <summary>Set the class loader that will be used to load the various objects.</summary>
		/// <param name="classLoader">the new class loader.</param>
		public virtual void setClassLoader(java.lang.ClassLoader classLoader)
		{
			this.classLoader = classLoader;
		}

		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			sb.Append("Configuration: ");
			if (loadDefaults)
			{
				toString(defaultResources, sb);
				if (resources.Count > 0)
				{
					sb.Append(", ");
				}
			}
			toString(resources, sb);
			return sb.ToString();
		}

		private void toString<T>(System.Collections.Generic.IList<T> resources, java.lang.StringBuilder
			 sb)
		{
			java.util.ListIterator<T> i = resources.listIterator();
			while (i.MoveNext())
			{
				if (i.nextIndex() != 0)
				{
					sb.Append(", ");
				}
				sb.Append(i.Current);
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
		public virtual void setQuietMode(bool quietmode)
		{
			lock (this)
			{
				this.quietmode = quietmode;
			}
		}

		internal virtual bool getQuietMode()
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
			new org.apache.hadoop.conf.Configuration().writeXml(System.Console.Out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			clear();
			int size = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			for (int i = 0; i < size; ++i)
			{
				string key = org.apache.hadoop.io.Text.readString(@in);
				string value = org.apache.hadoop.io.Text.readString(@in);
				set(key, value);
				string[] sources = org.apache.hadoop.io.WritableUtils.readCompressedStringArray(@in
					);
				if (sources != null)
				{
					updatingResource[key] = sources;
				}
			}
		}

		//@Override
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			java.util.Properties props = getProps();
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, props.Count);
			foreach (System.Collections.Generic.KeyValuePair<object, object> item in props)
			{
				org.apache.hadoop.io.Text.writeString(@out, (string)item.Key);
				org.apache.hadoop.io.Text.writeString(@out, (string)item.Value);
				org.apache.hadoop.io.WritableUtils.writeCompressedStringArray(@out, updatingResource
					[item.Key]);
			}
		}

		/// <summary>get keys matching the the regex</summary>
		/// <param name="regex"/>
		/// <returns>Map<String,String> with matching keys</returns>
		public virtual System.Collections.Generic.IDictionary<string, string> getValByRegex
			(string regex)
		{
			java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
			System.Collections.Generic.IDictionary<string, string> result = new System.Collections.Generic.Dictionary
				<string, string>();
			java.util.regex.Matcher m;
			foreach (System.Collections.Generic.KeyValuePair<object, object> item in getProps
				())
			{
				if (item.Key is string && item.Value is string)
				{
					m = p.matcher((string)item.Key);
					if (m.find())
					{
						// match
						result[(string)item.Key] = substituteVars(getProps().getProperty((string)item.Key
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

		public static void dumpDeprecatedKeys()
		{
			org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
				.get();
			foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.conf.Configuration.DeprecatedKeyInfo
				> entry in deprecations.getDeprecatedKeyMap())
			{
				java.lang.StringBuilder newKeys = new java.lang.StringBuilder();
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
		public static bool hasWarnedDeprecation(string name)
		{
			org.apache.hadoop.conf.Configuration.DeprecationContext deprecations = deprecationContext
				.get();
			if (deprecations.getDeprecatedKeyMap().Contains(name))
			{
				if (deprecations.getDeprecatedKeyMap()[name].accessed.get())
				{
					return true;
				}
			}
			return false;
		}
	}
}
