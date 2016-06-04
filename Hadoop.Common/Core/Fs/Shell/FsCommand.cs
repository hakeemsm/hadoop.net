using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Stat = Org.Apache.Hadoop.FS.Shell.Stat;

namespace Hadoop.Common.Core.Fs.Shell
{
	/// <summary>Base class for all "hadoop fs" commands</summary>
	public abstract class FsCommand : Command
	{
		// this class may not look useful now, but it's a placeholder for future
		// functionality to act as a registry for fs commands.  currently it's being
		// used to implement unnecessary abstract methods in the base class
		/// <summary>Register the command classes used by the fs subcommand</summary>
		/// <param name="factory">where to register the class</param>
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.RegisterCommands(typeof(AclCommands));
			factory.RegisterCommands(typeof(CopyCommands));
			factory.RegisterCommands(typeof(Count));
			factory.RegisterCommands(typeof(Delete));
			factory.RegisterCommands(typeof(Display));
			factory.RegisterCommands(typeof(Org.Apache.Hadoop.FS.Shell.Find.Find));
			factory.RegisterCommands(typeof(FsShellPermissions));
			factory.RegisterCommands(typeof(FsUsage));
			factory.RegisterCommands(typeof(LS));
			factory.RegisterCommands(typeof(Mkdir));
			factory.RegisterCommands(typeof(MoveCommands));
			factory.RegisterCommands(typeof(SetReplication));
			factory.RegisterCommands(typeof(Stat));
			factory.RegisterCommands(typeof(Tail));
			factory.RegisterCommands(typeof(Org.Apache.Hadoop.FS.Shell.Test));
			factory.RegisterCommands(typeof(Touch));
			factory.RegisterCommands(typeof(Truncate));
			factory.RegisterCommands(typeof(SnapshotCommands));
			factory.RegisterCommands(typeof(XAttrCommands));
		}

		protected internal FsCommand()
		{
		}

		protected internal FsCommand(Configuration conf)
			: base(conf)
		{
		}

		// historical abstract method in Command
		public override string GetCommandName()
		{
			return GetName();
		}

		// abstract method that normally is invoked by runall() which is
		// overridden below
		/// <exception cref="System.IO.IOException"/>
		protected internal override void Run(Path path)
		{
			throw new RuntimeException("not supposed to get here");
		}

		[Obsolete(@"use Command.Run(string[])")]
		public override int RunAll()
		{
			return Run(args);
		}
	}
}
