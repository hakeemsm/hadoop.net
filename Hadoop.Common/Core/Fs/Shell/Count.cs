using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Count the number of directories, files, bytes, quota, and remaining quota.
	/// 	</summary>
	public class Count : FsCommand
	{
		/// <summary>Register the names for the count command</summary>
		/// <param name="factory">the command factory that will instantiate this class</param>
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Count), "-count");
		}

		private const string OptionQuota = "q";

		private const string OptionHuman = "h";

		public const string Name = "count";

		public const string Usage = "[-" + OptionQuota + "] [-" + OptionHuman + "] <path> ...";

		public const string Description = "Count the number of directories, files and bytes under the paths\n"
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
		[System.ObsoleteAttribute(@"invoke via Org.Apache.Hadoop.FS.FsShell")]
		public Count(string[] cmd, int pos, Configuration conf)
			: base(conf)
		{
			this.args = Arrays.CopyOfRange(cmd, pos, cmd.Length);
		}

		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, int.MaxValue, OptionQuota, OptionHuman);
			cf.Parse(args);
			if (args.IsEmpty())
			{
				// default path is the current working directory
				args.AddItem(".");
			}
			showQuotas = cf.GetOpt(OptionQuota);
			humanReadable = cf.GetOpt(OptionHuman);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData src)
		{
			ContentSummary summary = src.fs.GetContentSummary(src.path);
			@out.WriteLine(summary.ToString(showQuotas, IsHumanReadable()) + src);
		}

		/// <summary>Should quotas get shown as part of the report?</summary>
		/// <returns>if quotas should be shown then true otherwise false</returns>
		[InterfaceAudience.Private]
		internal virtual bool IsShowQuotas()
		{
			return showQuotas;
		}

		/// <summary>Should sizes be shown in human readable format rather than bytes?</summary>
		/// <returns>true if human readable format</returns>
		[InterfaceAudience.Private]
		internal virtual bool IsHumanReadable()
		{
			return humanReadable;
		}
	}
}
