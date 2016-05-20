using Sharpen;

namespace org.apache.hadoop.conf
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

		public static System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			> getChangedProperties(org.apache.hadoop.conf.Configuration newConf, org.apache.hadoop.conf.Configuration
			 oldConf)
		{
			System.Collections.Generic.IDictionary<string, org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				> changes = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				>();
			// iterate over old configuration
			foreach (System.Collections.Generic.KeyValuePair<string, string> oldEntry in oldConf)
			{
				string prop = oldEntry.Key;
				string oldVal = oldEntry.Value;
				string newVal = newConf.getRaw(prop);
				if (newVal == null || !newVal.Equals(oldVal))
				{
					changes[prop] = new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange(prop
						, newVal, oldVal);
				}
			}
			// now iterate over new configuration
			// (to look for properties not present in old conf)
			foreach (System.Collections.Generic.KeyValuePair<string, string> newEntry in newConf)
			{
				string prop = newEntry.Key;
				string newVal = newEntry.Value;
				if (oldConf.get(prop) == null)
				{
					changes[prop] = new org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange(prop
						, newVal, null);
				}
			}
			return changes.Values;
		}

		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			> parseChangedProperties(org.apache.hadoop.conf.Configuration newConf, org.apache.hadoop.conf.Configuration
			 oldConf)
		{
			return getChangedProperties(newConf, oldConf);
		}
	}
}
