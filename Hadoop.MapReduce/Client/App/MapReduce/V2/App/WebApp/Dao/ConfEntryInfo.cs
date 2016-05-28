using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class ConfEntryInfo
	{
		protected internal string name;

		protected internal string value;

		protected internal string[] source;

		public ConfEntryInfo()
		{
		}

		public ConfEntryInfo(string key, string value)
			: this(key, value, null)
		{
		}

		public ConfEntryInfo(string key, string value, string[] source)
		{
			this.name = key;
			this.value = value;
			this.source = source;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual string GetValue()
		{
			return this.value;
		}

		public virtual string[] GetSource()
		{
			return source;
		}
	}
}
