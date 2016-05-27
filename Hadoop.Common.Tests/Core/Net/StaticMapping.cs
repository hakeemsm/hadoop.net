using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// Implements the
	/// <see cref="DNSToSwitchMapping"/>
	/// via static mappings. Used
	/// in testcases that simulate racks, and in the
	/// <see cref="Org.Apache.Hadoop.Hdfs.MiniDFSCluster"/>
	/// A shared, static mapping is used; to reset it call
	/// <see cref="ResetMap()"/>
	/// .
	/// When an instance of the class has its
	/// <see cref="SetConf(Org.Apache.Hadoop.Conf.Configuration)"/>
	/// method called, nodes listed in the configuration will be added to the map.
	/// These do not get removed when the instance is garbage collected.
	/// The switch mapping policy of this class is the same as for the
	/// <see cref="ScriptBasedMapping"/>
	/// -the presence of a non-empty topology script.
	/// The script itself is not used.
	/// </summary>
	public class StaticMapping : AbstractDNSToSwitchMapping
	{
		/// <summary>
		/// Key to define the node mapping as a comma-delimited list of host=rack
		/// mappings, e.g.
		/// </summary>
		/// <remarks>
		/// Key to define the node mapping as a comma-delimited list of host=rack
		/// mappings, e.g. <code>host1=r1,host2=r1,host3=r2</code>.
		/// <p/>
		/// Value:
		/// <value/>
		/// <p/>
		/// <b>Important: </b>spaces not trimmed and are considered significant.
		/// </remarks>
		public const string KeyHadoopConfiguredNodeMapping = "hadoop.configured.node.mapping";

		/// <summary>
		/// Configure the mapping by extracting any mappings defined in the
		/// <see cref="KeyHadoopConfiguredNodeMapping"/>
		/// field
		/// </summary>
		/// <param name="conf">new configuration</param>
		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			if (conf != null)
			{
				string[] mappings = conf.GetStrings(KeyHadoopConfiguredNodeMapping);
				if (mappings != null)
				{
					foreach (string str in mappings)
					{
						string host = Sharpen.Runtime.Substring(str, 0, str.IndexOf('='));
						string rack = Sharpen.Runtime.Substring(str, str.IndexOf('=') + 1);
						AddNodeToRack(host, rack);
					}
				}
			}
		}

		/// <summary>
		/// retained lower case setter for compatibility reasons; relays to
		/// <see cref="SetConf(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// </summary>
		/// <param name="conf">new configuration</param>
		public virtual void Setconf(Configuration conf)
		{
			SetConf(conf);
		}

		private static readonly IDictionary<string, string> nameToRackMap = new Dictionary
			<string, string>();

		/* Only one instance per JVM */
		/// <summary>Add a node to the static map.</summary>
		/// <remarks>
		/// Add a node to the static map. The moment any entry is added to the map,
		/// the map goes multi-rack.
		/// </remarks>
		/// <param name="name">node name</param>
		/// <param name="rackId">rack ID</param>
		public static void AddNodeToRack(string name, string rackId)
		{
			lock (nameToRackMap)
			{
				nameToRackMap[name] = rackId;
			}
		}

		public override IList<string> Resolve(IList<string> names)
		{
			IList<string> m = new AList<string>();
			lock (nameToRackMap)
			{
				foreach (string name in names)
				{
					string rackId;
					if ((rackId = nameToRackMap[name]) != null)
					{
						m.AddItem(rackId);
					}
					else
					{
						m.AddItem(NetworkTopology.DefaultRack);
					}
				}
				return m;
			}
		}

		/// <summary>
		/// The switch policy of this mapping is driven by the same policy
		/// as the Scripted mapping: the presence of the script name in
		/// the configuration file
		/// </summary>
		/// <returns>false, always</returns>
		public override bool IsSingleSwitch()
		{
			return IsSingleSwitchByScriptPolicy();
		}

		/// <summary>Get a copy of the map (for diagnostics)</summary>
		/// <returns>a clone of the map or null for none known</returns>
		public override IDictionary<string, string> GetSwitchMap()
		{
			lock (nameToRackMap)
			{
				return new Dictionary<string, string>(nameToRackMap);
			}
		}

		public override string ToString()
		{
			return "static mapping with single switch = " + IsSingleSwitch();
		}

		/// <summary>Clear the map</summary>
		public static void ResetMap()
		{
			lock (nameToRackMap)
			{
				nameToRackMap.Clear();
			}
		}

		public override void ReloadCachedMappings()
		{
		}

		// reloadCachedMappings does nothing for StaticMapping; there is
		// nowhere to reload from since all data is in memory.
		public override void ReloadCachedMappings(IList<string> names)
		{
		}
		// reloadCachedMappings does nothing for StaticMapping; there is
		// nowhere to reload from since all data is in memory.
	}
}
