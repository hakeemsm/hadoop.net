using System;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Destination path parameter.</summary>
	public class DestinationParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "destination";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		private static string Validate(string str)
		{
			if (str == null || str.Equals(Default))
			{
				return null;
			}
			if (!str.StartsWith(Path.Separator))
			{
				throw new ArgumentException("Invalid parameter value: " + Name + " = \"" + str + 
					"\" is not an absolute path.");
			}
			return new Path(str).ToUri().GetPath();
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public DestinationParam(string str)
			: base(Domain, Validate(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
