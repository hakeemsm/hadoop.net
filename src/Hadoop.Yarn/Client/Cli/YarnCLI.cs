using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public abstract class YarnCLI : Configured, Tool
	{
		public const string StatusCmd = "status";

		public const string ListCmd = "list";

		public const string KillCmd = "kill";

		public const string MoveToQueueCmd = "movetoqueue";

		public const string HelpCmd = "help";

		protected internal TextWriter sysout;

		protected internal TextWriter syserr;

		protected internal YarnClient client;

		public YarnCLI()
			: base(new YarnConfiguration())
		{
			client = YarnClient.CreateYarnClient();
			client.Init(GetConf());
			client.Start();
		}

		public virtual void SetSysOutPrintStream(TextWriter sysout)
		{
			this.sysout = sysout;
		}

		public virtual void SetSysErrPrintStream(TextWriter syserr)
		{
			this.syserr = syserr;
		}

		public virtual YarnClient GetClient()
		{
			return client;
		}

		public virtual void SetClient(YarnClient client)
		{
			this.client = client;
		}

		public virtual void Stop()
		{
			this.client.Stop();
		}

		public abstract int Run(string[] arg1);
	}
}
