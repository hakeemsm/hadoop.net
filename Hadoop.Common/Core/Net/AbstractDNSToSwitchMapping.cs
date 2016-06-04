using System.Collections.Generic;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>This is a base class for DNS to Switch mappings.</summary>
	/// <remarks>
	/// This is a base class for DNS to Switch mappings. <p/> It is not mandatory to
	/// derive
	/// <see cref="DNSToSwitchMapping"/>
	/// implementations from it, but it is strongly
	/// recommended, as it makes it easy for the Hadoop developers to add new methods
	/// to this base class that are automatically picked up by all implementations.
	/// <p/>
	/// This class does not extend the <code>Configured</code>
	/// base class, and should not be changed to do so, as it causes problems
	/// for subclasses. The constructor of the <code>Configured</code> calls
	/// the
	/// <see cref="SetConf(Configuration)"/>
	/// method, which will call into the
	/// subclasses before they have been fully constructed.
	/// </remarks>
	public abstract class AbstractDNSToSwitchMapping : DNSToSwitchMapping, Configurable
	{
		private Configuration conf;

		/// <summary>Create an unconfigured instance</summary>
		protected internal AbstractDNSToSwitchMapping()
		{
		}

		/// <summary>Create an instance, caching the configuration file.</summary>
		/// <remarks>
		/// Create an instance, caching the configuration file.
		/// This constructor does not call
		/// <see cref="SetConf(Configuration)"/>
		/// ; if
		/// a subclass extracts information in that method, it must call it explicitly.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		protected internal AbstractDNSToSwitchMapping(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>
		/// Predicate that indicates that the switch mapping is known to be
		/// single-switch.
		/// </summary>
		/// <remarks>
		/// Predicate that indicates that the switch mapping is known to be
		/// single-switch. The base class returns false: it assumes all mappings are
		/// multi-rack. Subclasses may override this with methods that are more aware
		/// of their topologies.
		/// <p/>
		/// This method is used when parts of Hadoop need know whether to apply
		/// single rack vs multi-rack policies, such as during block placement.
		/// Such algorithms behave differently if they are on multi-switch systems.
		/// </p>
		/// </remarks>
		/// <returns>true if the mapping thinks that it is on a single switch</returns>
		public virtual bool IsSingleSwitch()
		{
			return false;
		}

		/// <summary>Get a copy of the map (for diagnostics)</summary>
		/// <returns>a clone of the map or null for none known</returns>
		public virtual IDictionary<string, string> GetSwitchMap()
		{
			return null;
		}

		/// <summary>
		/// Generate a string listing the switch mapping implementation,
		/// the mapping for every known node and the number of nodes and
		/// unique switches known about -each entry to a separate line.
		/// </summary>
		/// <returns>
		/// a string that can be presented to the ops team or used in
		/// debug messages.
		/// </returns>
		public virtual string DumpTopology()
		{
			IDictionary<string, string> rack = GetSwitchMap();
			StringBuilder builder = new StringBuilder();
			builder.Append("Mapping: ").Append(ToString()).Append("\n");
			if (rack != null)
			{
				builder.Append("Map:\n");
				ICollection<string> switches = new HashSet<string>();
				foreach (KeyValuePair<string, string> entry in rack)
				{
					builder.Append("  ").Append(entry.Key).Append(" -> ").Append(entry.Value).Append(
						"\n");
					switches.AddItem(entry.Value);
				}
				builder.Append("Nodes: ").Append(rack.Count).Append("\n");
				builder.Append("Switches: ").Append(switches.Count).Append("\n");
			}
			else
			{
				builder.Append("No topology information");
			}
			return builder.ToString();
		}

		protected internal virtual bool IsSingleSwitchByScriptPolicy()
		{
			return conf != null && conf.Get(CommonConfigurationKeys.NetTopologyScriptFileNameKey
				) == null;
		}

		/// <summary>
		/// Query for a
		/// <see cref="DNSToSwitchMapping"/>
		/// instance being on a single
		/// switch.
		/// <p/>
		/// This predicate simply assumes that all mappings not derived from
		/// this class are multi-switch.
		/// </summary>
		/// <param name="mapping">the mapping to query</param>
		/// <returns>
		/// true if the base class says it is single switch, or the mapping
		/// is not derived from this class.
		/// </returns>
		public static bool IsMappingSingleSwitch(DNSToSwitchMapping mapping)
		{
			return mapping != null && mapping is Org.Apache.Hadoop.Net.AbstractDNSToSwitchMapping
				 && ((Org.Apache.Hadoop.Net.AbstractDNSToSwitchMapping)mapping).IsSingleSwitch();
		}

		public abstract void ReloadCachedMappings();

		public abstract void ReloadCachedMappings(IList<string> arg1);

		public abstract IList<string> Resolve(IList<string> arg1);
	}
}
