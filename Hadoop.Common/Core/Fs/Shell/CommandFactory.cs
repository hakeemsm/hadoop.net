using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>class to search for and register commands</summary>
	public class CommandFactory : Configured
	{
		private IDictionary<string, Type> classMap = new Dictionary<string, Type>();

		private IDictionary<string, Command> objectMap = new Dictionary<string, Command>(
			);

		/// <summary>Factory constructor for commands</summary>
		public CommandFactory()
			: this(null)
		{
		}

		/// <summary>Factory constructor for commands</summary>
		/// <param name="conf">the hadoop configuration</param>
		public CommandFactory(Configuration conf)
			: base(conf)
		{
		}

		/// <summary>Invokes "static void registerCommands(CommandFactory)" on the given class.
		/// 	</summary>
		/// <remarks>
		/// Invokes "static void registerCommands(CommandFactory)" on the given class.
		/// This method abstracts the contract between the factory and the command
		/// class.  Do not assume that directly invoking registerCommands on the
		/// given class will have the same effect.
		/// </remarks>
		/// <param name="registrarClass">class to allow an opportunity to register</param>
		public virtual void RegisterCommands(Type registrarClass)
		{
			try
			{
				registrarClass.GetMethod("registerCommands", typeof(Org.Apache.Hadoop.FS.Shell.CommandFactory
					)).Invoke(null, this);
			}
			catch (Exception e)
			{
				throw new RuntimeException(StringUtils.StringifyException(e));
			}
		}

		/// <summary>
		/// Register the given class as handling the given list of command
		/// names.
		/// </summary>
		/// <param name="cmdClass">the class implementing the command names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		public virtual void AddClass(Type cmdClass, params string[] names)
		{
			foreach (string name in names)
			{
				classMap[name] = cmdClass;
			}
		}

		/// <summary>
		/// Register the given object as handling the given list of command
		/// names.
		/// </summary>
		/// <remarks>
		/// Register the given object as handling the given list of command
		/// names.  Avoid calling this method and use
		/// <see cref="AddClass(System.Type{T}, string[])"/>
		/// whenever possible to avoid
		/// startup overhead from excessive command object instantiations.  This
		/// method is intended only for handling nested non-static classes that
		/// are re-usable.  Namely -help/-usage.
		/// </remarks>
		/// <param name="cmdObject">the object implementing the command names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		public virtual void AddObject(Command cmdObject, params string[] names)
		{
			foreach (string name in names)
			{
				objectMap[name] = cmdObject;
				classMap[name] = null;
			}
		}

		// just so it shows up in the list of commands
		/// <summary>Returns an instance of the class implementing the given command.</summary>
		/// <remarks>
		/// Returns an instance of the class implementing the given command.  The
		/// class must have been registered via
		/// <see cref="AddClass(System.Type{T}, string[])"/>
		/// </remarks>
		/// <param name="cmd">name of the command</param>
		/// <returns>instance of the requested command</returns>
		public virtual Command GetInstance(string cmd)
		{
			return GetInstance(cmd, GetConf());
		}

		/// <summary>Get an instance of the requested command</summary>
		/// <param name="cmdName">name of the command to lookup</param>
		/// <param name="conf">the hadoop configuration</param>
		/// <returns>
		/// the
		/// <see cref="Command"/>
		/// or null if the command is unknown
		/// </returns>
		public virtual Command GetInstance(string cmdName, Configuration conf)
		{
			if (conf == null)
			{
				throw new ArgumentNullException("configuration is null");
			}
			Command instance = objectMap[cmdName];
			if (instance == null)
			{
				Type cmdClass = classMap[cmdName];
				if (cmdClass != null)
				{
					instance = ReflectionUtils.NewInstance(cmdClass, conf);
					instance.SetName(cmdName);
					instance.SetCommandFactory(this);
				}
			}
			return instance;
		}

		/// <summary>Gets all of the registered commands</summary>
		/// <returns>a sorted list of command names</returns>
		public virtual string[] GetNames()
		{
			string[] names = Sharpen.Collections.ToArray(classMap.Keys, new string[0]);
			Arrays.Sort(names);
			return names;
		}
	}
}
