using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>A thin wrappper around record field.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JField<T>
	{
		private string name;

		private T type;

		/// <summary>Creates a new instance of JField</summary>
		public JField(string name, T type)
		{
			this.type = type;
			this.name = name;
		}

		internal virtual string getName()
		{
			return name;
		}

		internal virtual T getType()
		{
			return type;
		}
	}
}
