using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>
	/// A class holding a name and value pair, used for specifying filters in
	/// <see cref="TimelineReader"/>
	/// .
	/// </summary>
	public class NameValuePair
	{
		internal string name;

		internal object value;

		public NameValuePair(string name, object value)
		{
			this.name = name;
			this.value = value;
		}

		/// <summary>Get the name.</summary>
		/// <returns>The name.</returns>
		public virtual string GetName()
		{
			return name;
		}

		/// <summary>Get the value.</summary>
		/// <returns>The value.</returns>
		public virtual object GetValue()
		{
			return value;
		}

		public override string ToString()
		{
			return "{ name: " + name + ", value: " + value + " }";
		}
	}
}
