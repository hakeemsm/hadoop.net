using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
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
	/// <see cref="CachedDNSToSwitchMapping.Resolve(System.Collections.Generic.IList{E})"
	/// 	/>
	/// will look up the address as defined in the
	/// mapping file. If no entry corresponding to the address is found, the value
	/// <c>/default-rack</c>
	/// is returned.
	/// </p>
	/// </summary>
	public class TableMapping : CachedDNSToSwitchMapping
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.TableMapping
			));

		public TableMapping()
			: base(new TableMapping.RawTableMapping())
		{
		}

		private TableMapping.RawTableMapping GetRawMapping()
		{
			return (TableMapping.RawTableMapping)rawMapping;
		}

		public override Configuration GetConf()
		{
			return GetRawMapping().GetConf();
		}

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			GetRawMapping().SetConf(conf);
		}

		public override void ReloadCachedMappings()
		{
			base.ReloadCachedMappings();
			GetRawMapping().ReloadCachedMappings();
		}

		private sealed class RawTableMapping : Configured, DNSToSwitchMapping
		{
			private IDictionary<string, string> map;

			private IDictionary<string, string> Load()
			{
				IDictionary<string, string> loadMap = new Dictionary<string, string>();
				string filename = GetConf().Get(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey
					, null);
				if (StringUtils.IsBlank(filename))
				{
					Log.Warn(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey + " not configured. "
						);
					return null;
				}
				try
				{
					using (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream
						(filename), Charsets.Utf8)))
					{
						string line = reader.ReadLine();
						while (line != null)
						{
							line = line.Trim();
							if (line.Length != 0 && line[0] != '#')
							{
								string[] columns = line.Split("\\s+");
								if (columns.Length == 2)
								{
									loadMap[columns[0]] = columns[1];
								}
								else
								{
									Log.Warn("Line does not have two columns. Ignoring. " + line);
								}
							}
							line = reader.ReadLine();
						}
					}
				}
				catch (Exception e)
				{
					Log.Warn(filename + " cannot be read.", e);
					return null;
				}
				return loadMap;
			}

			public IList<string> Resolve(IList<string> names)
			{
				lock (this)
				{
					if (map == null)
					{
						map = Load();
						if (map == null)
						{
							Log.Warn("Failed to read topology table. " + NetworkTopology.DefaultRack + " will be used for all nodes."
								);
							map = new Dictionary<string, string>();
						}
					}
					IList<string> results = new AList<string>(names.Count);
					foreach (string name in names)
					{
						string result = map[name];
						if (result != null)
						{
							results.AddItem(result);
						}
						else
						{
							results.AddItem(NetworkTopology.DefaultRack);
						}
					}
					return results;
				}
			}

			public void ReloadCachedMappings()
			{
				IDictionary<string, string> newMap = Load();
				if (newMap == null)
				{
					Log.Error("Failed to reload the topology table.  The cached " + "mappings will not be cleared."
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

			public void ReloadCachedMappings(IList<string> names)
			{
				// TableMapping has to reload all mappings at once, so no chance to 
				// reload mappings on specific nodes
				ReloadCachedMappings();
			}
		}
	}
}
