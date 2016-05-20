using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Base class for all "hadoop fs" commands</summary>
	public abstract class FsCommand : org.apache.hadoop.fs.shell.Command
	{
		// this class may not look useful now, but it's a placeholder for future
		// functionality to act as a registry for fs commands.  currently it's being
		// used to implement unnecessary abstract methods in the base class
		/// <summary>Register the command classes used by the fs subcommand</summary>
		/// <param name="factory">where to register the class</param>
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.AclCommands
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CopyCommands
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Count
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Delete
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Display
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Find
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FsShellPermissions
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.FsUsage
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Ls
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Mkdir
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.MoveCommands
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SetReplication
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Stat
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Tail
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Test
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Touch
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Truncate
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SnapshotCommands
				)));
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.XAttrCommands
				)));
		}

		protected internal FsCommand()
		{
		}

		protected internal FsCommand(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		// historical abstract method in Command
		public override string getCommandName()
		{
			return getName();
		}

		// abstract method that normally is invoked by runall() which is
		// overridden below
		/// <exception cref="System.IO.IOException"/>
		protected internal override void run(org.apache.hadoop.fs.Path path)
		{
			throw new System.Exception("not supposed to get here");
		}

		[System.ObsoleteAttribute(@"use Command.run(string[])")]
		public override int runAll()
		{
			return run(args);
		}
	}
}
