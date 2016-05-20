using Sharpen;

namespace org.apache.hadoop.record.compiler
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

		public const string RIO_PREFIX = "_rio_";

		public const string RTI_VAR = RIO_PREFIX + "recTypeInfo";

		public const string RTI_FILTER = RIO_PREFIX + "rtiFilter";

		public const string RTI_FILTER_FIELDS = RIO_PREFIX + "rtiFilterFields";

		public const string RECORD_OUTPUT = RIO_PREFIX + "a";

		public const string RECORD_INPUT = RIO_PREFIX + "a";

		public const string TAG = RIO_PREFIX + "tag";
		// prefix to use for variables in generated classes
		// other vars used in generated classes
	}
}
