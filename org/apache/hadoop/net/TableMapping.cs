using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// <p>
	/// Simple
	/// <see cref="DNSToSwitchMapping"/>
	/// implementation that reads a 2 column text
	/// file. The columns are separated by whitespace. The first column is a DNS or
	/// IP address and the second column specifies the rack where the address maps.
	/// </p>
	/// <p>
	/// This class uses the configuration parameter
	/// <c>net.topology.table.file.name</c>
	/// to locate the mapping file.
	/// </p>
	/// <p>
	/// Calls to
	/// <see cref="CachedDNSToSwitchMapping.resolve(System.Collections.Generic.IList{E})"
	/// 	/>
	/// will look up the address as defined in the
	/// mapping file. If no entry corresponding to the address is found, the value
	/// <c>/default-rack</c>
	/// is returned.
	/// </p>
	/// </summary>
	public class TableMapping : org.apache.hadoop.net.CachedDNSToSwitchMapping
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.TableMapping
			)));

		public TableMapping()
			: base(new org.apache.hadoop.net.TableMapping.RawTableMapping())
		{
		}

		private org.apache.hadoop.net.TableMapping.RawTableMapping getRawMapping()
		{
			return (org.apache.hadoop.net.TableMapping.RawTableMapping)rawMapping;
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return getRawMapping().getConf();
		}

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			base.setConf(conf);
			getRawMapping().setConf(conf);
		}

		public override void reloadCachedMappings()
		{
			base.reloadCachedMappings();
			getRawMapping().reloadCachedMappings();
		}

		private sealed class RawTableMapping : org.apache.hadoop.conf.Configured, org.apache.hadoop.net.DNSToSwitchMapping
		{
			private System.Collections.Generic.IDictionary<string, string> map;

			private System.Collections.Generic.IDictionary<string, string> load()
			{
				System.Collections.Generic.IDictionary<string, string> loadMap = new System.Collections.Generic.Dictionary
					<string, string>();
				string filename = getConf().get(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);
				if (org.apache.commons.lang.StringUtils.isBlank(filename))
				{
					LOG.warn(org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY
						 + " not configured. ");
					return null;
				}
				try
				{
					using (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader
						(new java.io.FileInputStream(filename), org.apache.commons.io.Charsets.UTF_8)))
					{
						string line = reader.readLine();
						while (line != null)
						{
							line = line.Trim();
							if (line.Length != 0 && line[0] != '#')
							{
								string[] columns = line.split("\\s+");
								if (columns.Length == 2)
								{
									loadMap[columns[0]] = columns[1];
								}
								else
								{
									LOG.warn("Line does not have two columns. Ignoring. " + line);
								}
							}
							line = reader.readLine();
						}
					}
				}
				catch (System.Exception e)
				{
					LOG.warn(filename + " cannot be read.", e);
					return null;
				}
				return loadMap;
			}

			public System.Collections.Generic.IList<string> resolve(System.Collections.Generic.IList
				<string> names)
			{
				lock (this)
				{
					if (map == null)
					{
						map = load();
						if (map == null)
						{
							LOG.warn("Failed to read topology table. " + org.apache.hadoop.net.NetworkTopology
								.DEFAULT_RACK + " will be used for all nodes.");
							map = new System.Collections.Generic.Dictionary<string, string>();
						}
					}
					System.Collections.Generic.IList<string> results = new System.Collections.Generic.List
						<string>(names.Count);
					foreach (string name in names)
					{
						string result = map[name];
						if (result != null)
						{
							results.add(result);
						}
						else
						{
							results.add(org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK);
						}
					}
					return results;
				}
			}

			public void reloadCachedMappings()
			{
				System.Collections.Generic.IDictionary<string, string> newMap = load();
				if (newMap == null)
				{
					LOG.error("Failed to reload the topology table.  The cached " + "mappings will not be cleared."
						);
				}
				else
				{
					lock (this)
					{
						map = newMap;
					}
				}
			}

			public void reloadCachedMappings(System.Collections.Generic.IList<string> names)
			{
				// TableMapping has to reload all mappings at once, so no chance to 
				// reload mappings on specific nodes
				reloadCachedMappings();
			}
		}
	}
}
