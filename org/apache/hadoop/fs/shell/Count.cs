using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Count the number of directories, files, bytes, quota, and remaining quota.
	/// 	</summary>
	public class Count : org.apache.hadoop.fs.shell.FsCommand
	{
		/// <summary>Register the names for the count command</summary>
		/// <param name="factory">the command factory that will instantiate this class</param>
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Count
				)), "-count");
		}

		private const string OPTION_QUOTA = "q";

		private const string OPTION_HUMAN = "h";

		public const string NAME = "count";

		public const string USAGE = "[-" + OPTION_QUOTA + "] [-" + OPTION_HUMAN + "] <path> ...";

		public const string DESCRIPTION = "Count the number of directories, files and bytes under the paths\n"
			 + "that match the specified file pattern.  The output columns are:\n" + "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or\n"
			 + "QUOTA REMAINING_QUOTA SPACE_QUOTA REMAINING_SPACE_QUOTA \n" + "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME\n"
			 + "The -h option shows file sizes in human readable format.";

		private bool showQuotas;

		private bool humanReadable;

		/// <summary>Constructor</summary>
		public Count()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="cmd">the count command</param>
		/// <param name="pos">the starting index of the arguments</param>
		/// <param name="conf">configuration</param>
		[System.ObsoleteAttribute(@"invoke via org.apache.hadoop.fs.FsShell")]
		public Count(string[] cmd, int pos, org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			this.args = java.util.Arrays.copyOfRange(cmd, pos, cmd.Length);
		}

		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, int.MaxValue, OPTION_QUOTA, OPTION_HUMAN);
			cf.parse(args);
			if (args.isEmpty())
			{
				// default path is the current working directory
				args.add(".");
			}
			showQuotas = cf.getOpt(OPTION_QUOTA);
			humanReadable = cf.getOpt(OPTION_HUMAN);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			src)
		{
			org.apache.hadoop.fs.ContentSummary summary = src.fs.getContentSummary(src.path);
			@out.WriteLine(summary.toString(showQuotas, isHumanReadable()) + src);
		}

		/// <summary>Should quotas get shown as part of the report?</summary>
		/// <returns>if quotas should be shown then true otherwise false</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal virtual bool isShowQuotas()
		{
			return showQuotas;
		}

		/// <summary>Should sizes be shown in human readable format rather than bytes?</summary>
		/// <returns>true if human readable format</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal virtual bool isHumanReadable()
		{
			return humanReadable;
		}
	}
}
