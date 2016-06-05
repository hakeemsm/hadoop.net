using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Buffer size parameter.</summary>
	public class BufferSizeParam : IntegerParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "buffersize";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly IntegerParam.Domain Domain = new IntegerParam.Domain(Name
			);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public BufferSizeParam(int value)
			: base(Domain, value, 1, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public BufferSizeParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the value or, if it is null, return the default from conf.</returns>
		public virtual int GetValue(Configuration conf)
		{
			return GetValue() != null ? GetValue() : conf.GetInt(CommonConfigurationKeysPublic
				.IoFileBufferSizeKey, CommonConfigurationKeysPublic.IoFileBufferSizeDefault);
		}
	}
}
