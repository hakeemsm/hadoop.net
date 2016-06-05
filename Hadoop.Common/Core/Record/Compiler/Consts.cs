

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>const definitions for Record I/O compiler</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class Consts
	{
		/// <summary>Cannot create a new instance</summary>
		private Consts()
		{
		}

		public const string RioPrefix = "_rio_";

		public const string RtiVar = RioPrefix + "recTypeInfo";

		public const string RtiFilter = RioPrefix + "rtiFilter";

		public const string RtiFilterFields = RioPrefix + "rtiFilterFields";

		public const string RecordOutput = RioPrefix + "a";

		public const string RecordInput = RioPrefix + "a";

		public const string Tag = RioPrefix + "tag";
		// prefix to use for variables in generated classes
		// other vars used in generated classes
	}
}
