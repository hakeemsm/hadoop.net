using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class ReconfigurationUtil
	{
		public class PropertyChange
		{
			public string prop;

			public string oldVal;

			public string newVal;

			public PropertyChange(string prop, string newVal, string oldVal)
			{
				this.prop = prop;
				this.newVal = newVal;
				this.oldVal = oldVal;
			}
		}

		public static ICollection<ReconfigurationUtil.PropertyChange> GetChangedProperties
			(Configuration newConf, Configuration oldConf)
		{
			IDictionary<string, ReconfigurationUtil.PropertyChange> changes = new Dictionary<
				string, ReconfigurationUtil.PropertyChange>();
			// iterate over old configuration
			foreach (KeyValuePair<string, string> oldEntry in oldConf)
			{
				string prop = oldEntry.Key;
				string oldVal = oldEntry.Value;
				string newVal = newConf.GetRaw(prop);
				if (newVal == null || !newVal.Equals(oldVal))
				{
					changes[prop] = new ReconfigurationUtil.PropertyChange(prop, newVal, oldVal);
				}
			}
			// now iterate over new configuration
			// (to look for properties not present in old conf)
			foreach (KeyValuePair<string, string> newEntry in newConf)
			{
				string prop = newEntry.Key;
				string newVal = newEntry.Value;
				if (oldConf.Get(prop) == null)
				{
					changes[prop] = new ReconfigurationUtil.PropertyChange(prop, newVal, null);
				}
			}
			return changes.Values;
		}

		public virtual ICollection<ReconfigurationUtil.PropertyChange> ParseChangedProperties
			(Configuration newConf, Configuration oldConf)
		{
			return GetChangedProperties(newConf, oldConf);
		}
	}
}
